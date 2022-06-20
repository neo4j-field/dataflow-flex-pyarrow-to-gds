import logging
from collections import namedtuple
from distutils.util import strtobool

import apache_beam as beam

import pyarrow as pa
import pyarrow.flight as flight

from neo4j_arrow import Neo4jArrowClient
from neo4j_arrow.model import Node, Edge, Graph

from typing import (
    cast, Any, Dict, Iterable, Generator, List, Optional, Tuple, Union
)

# A common container type for carrying results.
Neo4jResult = namedtuple('Neo4jResult', ['count', 'nbytes', 'kind'])

# type aliases to tighten up function signatures
Arrow = Union[pa.Table, pa.RecordBatch]
KeyedArrow = Union[Tuple[str, Arrow], Arrow]
ArrowResult = Generator[KeyedArrow, None, None]
Neo4jResults = Generator[
    Union[Tuple[Any, Neo4jResult], Neo4jResult], None, None
]


def sum_results(results: Iterable[Neo4jResult], *,
                kind: Optional[str] = None) -> Neo4jResult:
    """Simple summation over Neo4jResults."""
    count, nbytes = 0, 0

    for result in results:
        count += result.count
        nbytes += result.nbytes
        if not kind:
            kind = result.kind
    return Neo4jResult(count, nbytes, kind)


class Signal(beam.DoFn):
    """
    Signal a completion event to Neo4j Arrow Flight Service.
    XXX: should be used after a global window combiner (can this be asserted?)
    """

    def __init__(self, client: Neo4jArrowClient, method_name: str, *out):
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
        else: # otherwise let our input pass through
            yield result


class CopyKeyToMetadata(beam.DoFn):
    """Copy a PCollection's key to the Arrow Table/RecordBatch's metadata."""
    def __init__(self, *, drop_key: bool = True, metadata_field: str = "source"):
        self.drop_key = drop_key
        self.metadata_field = metadata_field

    def process(self, elements: Tuple[str, Arrow]) -> ArrowResult:
        key, value = elements[0], elements[1]
        schema = value.schema.with_metadata({self.metadata_field: key})
        result = value.from_arrays(value.columns, schema=schema)
        if self.drop_key:
            yield result
        else:
            yield key, result


class WriteEdges(beam.DoFn):
    """Stream a PyArrow Table/RecordBatch of Edges to the Neo4j GDS server"""

    def __init__(self, client: Neo4jArrowClient, model: Graph):
        self.client = client.copy() # makes a shallow copy that's serializable
        self.model = model

    def process(self, elements: KeyedArrow) -> Neo4jResults:
        key = None
        if isinstance(elements, tuple):
            key, edges = cast(Any, elements[0]), cast(Arrow, elements[1])
        else:
            edges = cast(Arrow, elements)
        try:
            rows, nbytes = self.client.write_edges(edges, self.model)
            logging.debug(f"wrote {rows:,} rows, {nbytes:,} bytes")
            result = Neo4jResult(rows, nbytes, 'edge')
            if key:
                yield key, result
            else:
                yield result
        except Exception as e:
            logging.error("failed to write edge table: ", e)
            raise e


class WriteNodes(beam.DoFn):
    """Stream a PyArrow Table/RecordBatch of Nodes to the Neo4j GDS server"""

    def __init__(self, client: Neo4jArrowClient, model: Graph):
        self.client = client.copy() # makes a shallow copy that's serializable
        self.model = model

    def process(self, elements: KeyedArrow) -> Neo4jResults:
        key = None
        if isinstance(elements, tuple):
            key, nodes = cast(Any, elements[0]), cast(Arrow, elements[1])
        else:
            nodes = cast(Arrow, elements)
        try:
            rows, nbytes = self.client.write_nodes(nodes, self.model)
            logging.debug(f"wrote {rows:,} rows, {nbytes:,} bytes")
            result = Neo4jResult(rows, nbytes, 'node')
            if key:
                yield key, result
            else:
                yield result
        except Exception as e:
            logging.error("failed to write node table: ", e)
            raise e


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
