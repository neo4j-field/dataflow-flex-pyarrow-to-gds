import logging
from collections import namedtuple
from distutils.util import strtobool

import apache_beam as beam

import pyarrow as pa
import pyarrow.flight as flight
import neo4j_arrow as na

from typing import Any, Dict, Iterable, Generator, List, Tuple, Union

Nodes = Union[pa.Table, Iterable[pa.RecordBatch]]
Edges = Union[pa.Table, Iterable[pa.RecordBatch]]
Arrow = Union[pa.Table, pa.RecordBatch]
Neo4jResult = namedtuple('Neo4jResult', ['count', 'nbytes', 'kind'])


def sum_results(results: Iterable[Neo4jResult]) -> Neo4jResult:
    """Simple summation over Neo4jResults."""
    count, nbytes = 0, 0
    kind = ''
    for result in results:
        count += result.count
        nbytes += result.nbytes
        kind = result.kind
    return Neo4jResult(count, nbytes, kind)


class Signal(beam.DoFn):
    """
    Signal a completion event to Neo4j Arrow Flight Service.
    XXX: should be used after a global window combiner
    """

    def __init__(self, client: na.Neo4jArrowClient, method_name: str, *out):
        self.client = client.copy()
        self.method_name = method_name
        self.method = getattr(self.client, method_name)
        self.out = out

    def process(self, result: Neo4jResult) -> Generator[Any, None, None]:
        response = self.method()
        logging.info(f"called '{self.method_name}', response: {response}")

        if self.out: # pass through any provided side input
            for val in self.out:
                yield val


class WriteEdges(beam.DoFn):
    """Stream a PyArrow Table of Edges to the Neo4j GDS server"""

    def __init__(self, client: na.Neo4jArrowClient, mappingfn = None):
        self.client = client.copy() # makes a shallow copy that's serializable
        self.mappingfn = mappingfn

    def process(self, edges: Edges) -> Generator[Neo4jResult, None, None]:
        try:
            rows, nbytes = self.client.write_edges(edges, self.mappingfn)
            logging.debug(f"wrote {rows:,} rows, {nbytes:,} bytes")
            yield Neo4jResult(rows, nbytes, 'edge')
        except Exception as e:
            logging.error("failed to write edge table: ", e)
            raise e
        # yield Neo4jResult(0, 0, 'edge')


class WriteNodes(beam.DoFn):
    """Stream a PyArrow Table of Nodes to the Neo4j GDS server"""

    def __init__(self, client: na.Neo4jArrowClient, mappingfn = None):
        self.client = client.copy() # makes a shallow copy that's serializable
        self.mappingfn = mappingfn

    def process(self, nodes: Nodes) -> Generator[Neo4jResult, None, None]:
        try:
            rows, nbytes = self.client.write_nodes(nodes, self.mappingfn)
            logging.debug(f"wrote {rows:,} rows, {nbytes:,} bytes")
            yield Neo4jResult(rows, nbytes, 'node')
        except Exception as e:
            logging.error("failed to write node table: ", e)
            raise e
        # yield Neo4jResult(0, 0, 'node')


class Echo(beam.DoFn):
    """Log a value and pass it to the next transform."""
    def __init__(self, level: int = logging.INFO, prefix: str = ''):
        if prefix:
            self.prefix = prefix + ' ' # just a little whitespace
        else:
            self.prefix = ''
        self.level = level

    def process(self, value) -> Generator[Any, None, None]:
        logging.log(self.level, f"{self.prefix}{value}")
        yield value


def map_nodes(batch: Arrow) -> Arrow:
    """TODO: pull this out and make dynamic."""
    new_schema = batch.schema
    for idx, name in enumerate(batch.schema.names):
        field = new_schema.field(name)
        if name in ["author", "paper", "institution"]:
            new_schema = new_schema.set(idx, field.with_name("nodeId"))
    return batch.from_arrays(batch.columns, schema=new_schema)


def map_edges(batch: Arrow) -> Arrow:
    """TODO: pull this out and make dynamic."""
    new_schema = batch.schema

    # XXX: temp hackjob, hardcoded to my dataset
    my_type = batch["type"][0].as_py()
    if my_type == "AFFILIATED_WITH":
        src, tgt = ("author", "institution")
    elif my_type == "AUTHORED":
        src, tgt = ("author", "paper")
    elif my_type == "CITES":
        src, tgt = ("source", "target")
    else:
        raise Exception("invalid schema")

    for idx, name in enumerate(batch.schema.names):
        f = new_schema.field(name)
        if name == src:
            new_schema = new_schema.set(idx, f.with_name("sourceNodeId"))
        elif name == tgt:
            new_schema = new_schema.set(idx, f.with_name("targetNodeId"))
        elif name == "type":
            new_schema = new_schema.set(idx, f.with_name("relationshipType"))
    return batch.from_arrays(batch.columns, schema=new_schema)
