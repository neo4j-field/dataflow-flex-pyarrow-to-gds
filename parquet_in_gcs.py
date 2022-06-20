#!/usr/bin/env python3
import argparse
import logging
from logging import INFO, DEBUG

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pyarrow as pa

from neo4j_arrow import Neo4jArrowClient
from neo4j_arrow.model import Graph, Node, Edge

import neo4j_beam as nb
from neo4j_beam import (
    Echo, WriteEdges, WriteNodes, Signal, map_edges, map_nodes, sum_results
)

from typing import List


G = (
    Graph(name="test", db="neo4j")
    .with_node(Node("Paper", "labels", "paper"))
    .with_node(Node("Author", "labels", "author"))
    .with_node(Node("Institution", "labels", "institution"))
    .with_edge(Edge("CITES", "type", "source", "target"))
    .with_edge(Edge("AFFILIATED_WITH", "type", "author", "institution"))
    .with_edge(Edge("AUTHORED", "type", "author", "paper"))
)


def run(host: str, port: int, user: str, password: str, graph: str,
        database: str, tls: bool, concurrency: int,
        gcs_node_pattern: str, gcs_edge_pattern: str, graph_json: str,
        beam_args: List[str] = None) -> None:
    """It's Morbin time!"""
    options = PipelineOptions(beam_args, save_main_session=True)
    client = Neo4jArrowClient(host, graph, port=port, user=user,
                              password=password, tls=tls, database=database,
                              concurrency=concurrency)

    if graph_json:
        # TODO: Load our graph
        pass

    logging.info(f"Using graph model: {G}")

    logging.info(f"Starting job with {gcs_node_pattern} and {gcs_edge_pattern}")
    # "The Joys of Beam"
    with beam.Pipeline(options=options) as pipeline:
        client.start()
        node_result = (
            pipeline
            | "Begin loading nodes" >> beam.Create([gcs_node_pattern])
            | "Read node files" >> beam.io.ReadAllFromParquetBatched()
            | "Send nodes to Neo4j" >> beam.ParDo(WriteNodes(client, G))
            | "Sum node results" >> beam.CombineGlobally(sum_results)
            | "Echo node results" >> beam.ParDo(Echo(INFO, "node result:"))
        )
        nodes_done = (
            node_result
            | "Signal node completion" >> beam.ParDo(
                nb.Signal(client, "nodes_done", gcs_edge_pattern))
        )
        edge_result = (
            nodes_done
            | "Read edge files" >> beam.io.ReadAllFromParquetBatched()
            | "Send edges to Neo4j" >> beam.ParDo(WriteEdges(client, G))
            | "Sum edge results" >> beam.CombineGlobally(sum_results)
            | "Echo edge results" >> beam.ParDo(Echo(INFO, "edge result:"))
        )
        results = [
            beam.pvalue.AsSingleton(node_result),
            beam.pvalue.AsSingleton(edge_result)
        ]
        edges_done = (
            edge_result
            | "Signal edge completion" >> beam.ParDo(
                Signal(client, "edges_done", *results))
            | "Echo final results" >> beam.ParDo(Echo(INFO, "final results:"))
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
        type=nb.util.strtobool,
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
    parser.add_argument(
        "--graph_json",
        help="Path to a JSON representation of the Graph model.",
    )
    args, beam_args = parser.parse_known_args()

    # Make rocket go now...
    run(args.neo4j_host, args.neo4j_port, args.neo4j_user, args.neo4j_password,
        args.neo4j_graph, args.neo4j_database, args.neo4j_use_tls,
        args.neo4j_concurrency, args.gcs_node_pattern, args.gcs_edge_pattern,
        args.graph_json, beam_args)
