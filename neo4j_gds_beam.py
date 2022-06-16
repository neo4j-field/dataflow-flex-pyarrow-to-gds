#!/usr/bin/env python3
import argparse
import logging
from collections import namedtuple
from distutils.util import strtobool

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pyarrow as pa
import pyarrow.flight as flight
import neo4j_arrow as na

from typing import Any, Dict, Iterable, Generator, List, Tuple, Union

Nodes = Union[pa.Table, Iterable[pa.RecordBatch]]
Edges = Union[pa.Table, Iterable[pa.RecordBatch]]
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
        self.method = getattr(self.client, method_name)
        self.out = out

    def process(self, result: Neo4jResult):
        print(f"result: {result}")
        self.method()
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
            yield Neo4jResult(rows, nbytes, 'edge')
        except Exception as e:
            print(f"failed to write edge table: {e}")
            raise e
        yield Neo4jResult(0, 0, 'edge')


class WriteNodes(beam.DoFn):
    """Stream a PyArrow Table of Nodes to the Neo4j GDS server"""

    def __init__(self, client: na.Neo4jArrowClient, mappingfn = None):
        self.client = client.copy() # makes a shallow copy that's serializable
        self.mappingfn = mappingfn

    def process(self, nodes: Nodes) -> Generator[Neo4jResult, None, None]:
        try:
            rows, nbytes = self.client.write_nodes(nodes, self.mappingfn)
            yield Neo4jResult(rows, nbytes, 'node')
        except Exception as e:
            print(f"failed to write node table: {e}")
            raise e
        yield Neo4jResult(0, 0, 'node')


class Echo(beam.DoFn):
    """Log a value and pass it to the next transform."""
    def __init__(self, msg: str):
        self.msg = msg

    def process(self, value) -> Generator[Any, None, None]:
        print(f"{self.msg}: {value}")
        yield value


def run(host: str, port: int, user: str, password: str, graph: str,
        database: str, tls: bool, concurrency: int,
        gcs_node_pattern: str, gcs_edge_pattern: str,
        beam_args: List[str] = None) -> None:
    """It's Morbin time!"""
    options = PipelineOptions(beam_args, save_main_session=True)
    client = na.Neo4jArrowClient(host, graph, port=port, user=user,
                                 password=password, tls=tls, database=database,
                                 concurrency=concurrency)

    def map_nodes(batch: Union[pa.Table, pa.RecordBatch]):
        """TODO: pull this out and make dynamic."""
        new_schema = batch.schema
        for idx, name in enumerate(batch.schema.names):
            field = new_schema.field(name)
            if name in ["author", "paper", "institution"]:
                new_schema = new_schema.set(idx, field.with_name("nodeId"))
        return batch.from_arrays(batch.columns, schema=new_schema)

    def map_edges(batch: Union[pa.Table, pa.RecordBatch]):
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
            field = new_schema.field(name)
            if name == src:
                new_schema = new_schema.set(idx,
                                            field.with_name("sourceNodeId"))
            elif name == tgt:
                new_schema = new_schema.set(idx,
                                            field.with_name("targetNodeId"))
            elif name == "type":
                new_schema = new_schema.set(idx,
                                            field.with_name("relationshipType"))
        return batch.from_arrays(batch.columns, schema=new_schema)

    logging.info(f"Starting job with {gcs_node_pattern} and {gcs_edge_pattern}")
    client.start()
    # "The Joys of Beam"
    with beam.Pipeline(options=options) as pipeline:
        node_result = (
            pipeline
            | "Begin loading nodes" >> beam.Create([gcs_node_pattern])
            | "Read node files" >> beam.io.ReadAllFromParquetBatched()
            | "Send nodes to Neo4j" >> beam.ParDo(WriteNodes(client, map_nodes))
            | "Sum node results" >> beam.CombineGlobally(sum_results)
            | "Echo node results" >> beam.ParDo(Echo("node result"))
        )
        nodes_done = (
            node_result
            | "Signal node completion" >> beam.ParDo(
                Signal(client, "nodes_done", gcs_edge_pattern))
        )
        edge_result = (
            nodes_done
            | "Read edge files" >> beam.io.ReadAllFromParquetBatched()
            | "Send edges to Neo4j" >> beam.ParDo(WriteEdges(client, map_edges))
            | "Sum edge results" >> beam.CombineGlobally(sum_results)
            | "Echo edge results" >> beam.ParDo(Echo("edge result"))
        )
        results = [
            beam.pvalue.AsSingleton(node_result),
            beam.pvalue.AsSingleton(edge_result)
        ]
        edges_done = (
            edge_result
            | "Signal edge completion" >> beam.ParDo(
                Signal(client, "edges_done", *results))
            | "Echo final results" >> beam.ParDo(Echo("final results"))
        )
    logging.info(f"Finished creating graph '{graph}'.")


if __name__ == "__main__":
    # TODO: env toggle for log level?
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--neo4j_host",
        help="Hostname or IP address of Neo4j server.",
    )
    parser.add_argument(
        "--neo4j_port",
        default=8491,
        type=int,
        help="TCP Port of Neo4j Arrow Flight service.",
    )
    parser.add_argument(
        "--neo4j_use_tls",
        default="True",
        type=strtobool,
        help="Use TLS for encrypting Neo4j Arrow Flight connection.",
    )
    parser.add_argument(
        "--neo4j_user",
        default="neo4j",
        help="Neo4j username.",
    )
    parser.add_argument(
        "--neo4j_password",
        help="Password for given Neo4j user.",
    )
    parser.add_argument(
        "--neo4j_graph",
        help="Name of the resulting Neo4j Graph.",
    )
    parser.add_argument(
        "--neo4j_database",
        default="neo4j",
        help="Name of the parent Neo4j database.",
    )
    parser.add_argument(
        "--neo4j_concurrency",
        default=4,
        type=int,
        help="Neo4j server-side concurrency for data processing.",
    )
    parser.add_argument(
        "--gcs_node_pattern",
        help="GCS URI file pattern to node parquet files.",
    )
    parser.add_argument(
        "--gcs_edge_pattern",
        help="GCS URI file pattern to edge parquet files.",
    )
    args, beam_args = parser.parse_known_args()

    # Make rocket go now...
    run(args.neo4j_host, args.neo4j_port, args.neo4j_user, args.neo4j_password,
        args.neo4j_graph, args.neo4j_database, args.neo4j_use_tls,
        args.neo4j_concurrency, args.gcs_node_pattern, args.gcs_edge_pattern,
        beam_args)
