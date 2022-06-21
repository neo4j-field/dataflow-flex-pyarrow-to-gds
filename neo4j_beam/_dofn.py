import logging
from collections import namedtuple, abc
from distutils.util import strtobool

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq

import pyarrow as pa
import pyarrow.flight as flight

from neo4j_arrow import Neo4jArrowClient
from neo4j_arrow.model import Node, Edge, Graph

from neo4j_bigquery import BigQuerySource

from typing import (
    cast, Any, Dict, Iterable, Generator, List, Optional, Tuple, Union
)


# A common container type for carrying results.
Neo4jResult = namedtuple('Neo4jResult', ['count', 'nbytes', 'kind'])

# type aliases to tighten up function signatures
Arrow = Union[pa.Table, pa.RecordBatch]
StreamKey = Tuple[str, int]
TupleStream = Generator[Tuple[StreamKey, str], None, None]
KeyedArrow = Union[Tuple[Any, Arrow], Arrow]
KeyedArrowStream = Generator[Tuple[StreamKey, Arrow], None, None]
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

    def __init__(self, client: Neo4jArrowClient, method_name: str,
                 out: Optional[Union[Iterable[Any], Any]] = None):
        self.client = client.copy()
        self.method_name = method_name
        self.method = getattr(self.client, method_name)
        self.out = out

    def process(self, result: Neo4jResult) -> Generator[Any, None, None]:
        response = self.method()
        logging.info(f"called '{self.method_name}', response: {response}")

        if self.out: # pass through any provided side input
            if isinstance(self.out, (str, dict)):
                # Don't iterate over strings or dictionaries!
                yield self.out
            elif isinstance(self.out, abc.Iterable):
                for val in self.out:
                    yield val
            else: # Give up
                yield self.out
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

    def __init__(self, client: Neo4jArrowClient, model: Graph,
                 source_field: Optional[str] = None):
        self.client = client.copy() # makes a shallow copy that's serializable
        self.model = model
        self.source_field = source_field

    def process(self, elements: KeyedArrow) -> Neo4jResults:
        key = None
        if isinstance(elements, tuple):
            key, edges = cast(Any, elements[0]), cast(Arrow, elements[1])
        else:
            edges = cast(Arrow, elements)
        try:
            rows, nbytes = self.client.write_edges(edges, self.model,
                                                   self.source_field)
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

    def __init__(self, client: Neo4jArrowClient, model: Graph,
                 source_field: Optional[str] = None):
        self.client = client.copy() # makes a shallow copy that's serializable
        self.model = model
        self.source_field = source_field

    def process(self, elements: KeyedArrow) -> Neo4jResults:
        key = None
        if isinstance(elements, tuple):
            key, nodes = cast(Any, elements[0]), cast(Arrow, elements[1])
        else:
            nodes = cast(Arrow, elements)
        try:
            rows, nbytes = self.client.write_nodes(nodes, self.model,
                                                   self.source_field)
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


class GetBQStream(beam.DoFn):
    def __init__(self, bq_source: BigQuerySource):
        self.bq_source = bq_source

    def process(self, table: str) -> TupleStream:
        streams = self.bq_source.table(table)
        logging.info(f"GetBQStream: got {len(streams)} streams.")
        for idx, stream in enumerate(streams):
            yield ((table, idx), stream)


class ReadBQStream(beam.DoFn):
    def __init__(self, bq_source: BigQuerySource):
        self.bq_source = bq_source

    def process(self, keyed_stream: Tuple[StreamKey, str]) -> KeyedArrowStream:
        key, stream = keyed_stream
        table, _ = key
        batches = self.bq_source.consume_stream(stream)

        for batch in batches:
            assert isinstance(batch, pa.RecordBatch)
            rb = cast(pa.RecordBatch, batch)
            schema = rb.schema.with_metadata({"src": table})
            arrow = rb.from_arrays(rb.columns, schema=schema)
            yield (key, arrow)
