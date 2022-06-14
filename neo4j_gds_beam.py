#!/usr/bin/env python3
import argparse
import logging
from collections import namedtuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pyarrow as pa
import pyarrow.flight as flight
import neo4j_arrow as na

from typing import Any, Dict, List, Tuple, Union


Neo4jResult = namedtuple('Neo4jResult', ['count', 'nbytes'])


class WriteEdgeTable(beam.DoFn):
    """Stream a PyArrow Table of Edges to the Neo4j GDS server"""

    def __init__(self, client: na.Neo4jArrowClient):
        self.client = client.copy() # makes a shallow copy that's serializable

    def process(self, table: pa.Table) -> Neo4jResult:
        try:
            rows, nbytes = self.client.write_edges(table)
            return Neo4jResult(rows, nbytes)
        except Exception as e:
            print(f"failed to write edge table: {e}")
        return Neo4jResult(0, 0)


class WriteNodeTable(beam.DoFn):
    """Stream a PyArrow Table of Nodes to the Neo4j GDS server"""

    def __init__(self, client: na.Neo4jArrowClient):
        self.client = client.copy() # makes a shallow copy that's serializable

    def process(self, table: pa.Table) -> Neo4jResult:
        try:
            rows, nbytes = self.client.write_nodes(table)
            return Neo4jResult(rows, nbytes)
        except Exception as e:
            print(f"failed to write node table: {e}")
        return Neo4jResult(0, 0)


def run(neo4j_host: str, neo4j_port: int, neo4j_user: str, neo4j_password: str,
        neo4j_graph: str, neo4j_database: str, neo4j_concurrency: int,
        gcs_node_pattern: str, gcs_edge_pattern: str,
        beam_args: List[str] = None) -> None:
    """It's Morbin time!"""
    options = PipelineOptions(beam_args, save_main_session=True)
    client = na.Neo4jArrowClient(neo4j_host, neo4j_graph, port=neo4j_port,
                                 user=neo4j_user, password=neo4j_password,
                                 concurrency=neo4j_concurrency)

    cnt, nbytes = 0, 0
    client.start()

    with beam.Pipeline(options=options) as pipeline:
        nodes = (
            pipeline
            | "Read node files" >> beam.io.ReadFromParquetBatched(file_pattern=gcs_node_pattern)
            | "Send to Neo4j" >> beam.ParDo(WriteNodeTable(client))
        )
        cnt = (nodes | beam.Map(lambda x: x.count) | beam.CombineGlobally(sum))
        bytes = (nodes | beam.Map(lambda x: x.nbytes) | beam.CompileGlobally(sum))
    logging.info(f"Sent {cnt:,} nodes, {nbytes:,} bytes.")
    client.nodes_done()

    with beam.Pipeline(options=options) as pipeline:
        edges = (
            pipeline
            | "Read edge files" >> beam.io.ReadFromParquetBatched(file_pattern=gcs_edge_pattern)
            | "Send to Neo4j" >> beam.ParDo(WriteEdgeTable(client))
        )
        cnt = (edges | beam.Map(lambda x: x.count) | beam.CombineGlobally(sum))
        nbytes = (edges | beam.Map(lambda x: x.nbytes) | beam.CompileGlobally(sum))
    logging.info(f"Sent {cnt:,} nodes, {nbytes:,} bytes.")
    client.edges_done()
    logging.info(f"Finished creating graph '{neo4j_graph}'.")


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
        default=True,
        type=bool,
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

    run(args.neo4j_host, args.neo4j_port, args.neo4j_graph, args.neo4j_database,
        args.gcs_node_pattern, args.gcs_edge_pattern, beam_args)
