#!/usr/bin/env python3
import argparse
import logging
from logging import INFO, DEBUG

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pyarrow as pa

from neo4j_arrow import Neo4jArrowClient
from neo4j_arrow.model import Graph, Node, Edge

from neo4j_beam import (
    CopyKeyToMetadata, Echo, GetBQStream, Neo4jResult, ReadBQStream,
    WriteEdges, WriteNodes, Signal, sum_results, util
)

from neo4j_bigquery import BigQuerySource

from typing import cast, Any, List, Optional


def load_model_from_path(path: str) -> Optional[Graph]:
    """Attempt to load a Graph model from a local filesystem."""
    try:
        with open(path, mode='r') as f:
            lines = f.readlines()
            return Graph.from_json(''.join(lines))
    except Exception:
        logging.info(f"not a local file: {path}")
        return None


def load_model_from_gcs(uri: str) -> Optional[Graph]:
    """Attempt to load a Graph model from a GCS uri."""
    try:
        import google.cloud
        from google.cloud import storage # type: ignore
        from google.cloud.storage.blob import Blob

        gcs = storage.Client()
        blob = Blob.from_string(uri, client=gcs)
        payload = blob.download_as_text()
        return Graph.from_json(payload)
    except Exception as e:
        logging.info(f"could not load from uri: {uri}")
        logging.info(f"error: {e}")
        return None


def run_gcs_pipeline(g: Graph, client: Neo4jArrowClient, node_pattern: str,
                     edge_pattern: str, beam_args: List[str] = []):
    """Run a Beam pipeline for ingesting data from Parquet in GCS."""
    options = PipelineOptions(beam_args, save_main_session=True)

    logging.info(f"Using graph model: {g}")
    logging.info(f"Starting GCS job with {node_pattern} & {edge_pattern}")

    with beam.Pipeline(options=options) as pipeline:
        client.start()
        nodes_result = (
            pipeline
            | "Begin loading nodes" >> beam.Create([node_pattern])
            | "Read node files" >> beam.io.ReadAllFromParquetBatched(
                with_filename=True)
            | "Set node metadata" >> beam.ParDo(CopyKeyToMetadata(
                metadata_field="src", drop_key=True))
            | "Send nodes to Neo4j" >> beam.ParDo(WriteNodes(client, g, "src"))
            | "Sum node results" >> beam.CombineGlobally(sum_results)
        )
        nodes_done = (
            nodes_result
            | "Echo node results" >> beam.ParDo(Echo(INFO, "node result:"))
            | "Signal nodes done" >> beam.ParDo(
                Signal(client, "nodes_done", [edge_pattern]))
        )
        edges_result = (
            nodes_done
            | "Read edge files" >> beam.io.ReadAllFromParquetBatched(
                with_filename=True)
            | "Set edge metadata" >> beam.ParDo(CopyKeyToMetadata(
                metadata_field="src", drop_key=True))
            | "Send edges to Neo4j" >> beam.ParDo(WriteEdges(client, g, "src"))
            | "Sum edge results" >> beam.CombineGlobally(sum_results)
        )
        edges_done = (
            edges_result
            | "Echo edge results" >> beam.ParDo(Echo(INFO, "edge result:"))
            | "Signal edge done" >> beam.ParDo(Signal(client, "edges_done"))
        )
        results = (
            [nodes_result, edges_result]
            | "Flatten results" >> beam.Flatten()
            | "Compute final results" >> beam.CombineGlobally(sum_results)
            | "Override result kind" >> beam.Map(
                lambda r: Neo4jResult(r.count, r.nbytes, "final"))
            | "Echo final results" >> beam.ParDo(Echo(INFO, "final results:"))
        )
    logging.info(f"Finished creating graph '{g.name}' from Parquet files.")


def get_streams(bq: BigQuerySource, tables: List[str]):
    idx = 0
    results = []
    for table in tables:
        for stream in bq.table(table):
            results.append(((table, idx), stream))
            idx += 1
    return results


def run_bigquery_pipeline(g: Graph, client: Neo4jArrowClient,
                          node_streams: List[Any], edge_streams: List[Any],
                          bq: BigQuerySource, beam_args: List[str] = []):
    """Run a Beam pipeline for ingesting data from a BigQuery dataset."""
    options = PipelineOptions(beam_args, save_main_session=True)

    logging.info(f"Using graph model: {g}")
    logging.info(f"Starting BigQuery job with {len(node_streams)} node streams,"
                 f" {len(edge_streams)} edge streams")

    with beam.Pipeline(options=options) as pipeline:
        client.start()
        nodes_result = (
            pipeline
            | "Seed our node streams" >> beam.Create(node_streams)
            | "Read node BQ streams" >> beam.ParDo(ReadBQStream(bq, 50_000))
            | "Send nodes to Neo4j" >> beam.ParDo(WriteNodes(client, g, "src"))
            | "Drop node key" >> beam.Values()
            | "Sum node results" >> beam.CombineGlobally(sum_results)
        )
        nodes_done = (
            nodes_result
            | "Echo node results" >> beam.ParDo(Echo(INFO, "node result:"))
            | "Signal nodes done" >> beam.ParDo(
                Signal(client, "nodes_done", edge_streams))
        )
        edges_result = (
            nodes_done
            | "Read Edge BQ streams" >> beam.ParDo(ReadBQStream(bq, 50_000))
            | "Send edges to Neo4j" >> beam.ParDo(WriteEdges(client, g, "src"))
            | "Drop edge key" >> beam.Values()
            | "Sum edge results" >> beam.CombineGlobally(sum_results)
        )
        edges_done = (
            edges_result
            | "Echo edge results" >> beam.ParDo(Echo(INFO, "edge result:"))
            | "Signal edges done" >> beam.ParDo(Signal(client, "edges_done"))
        )
        results = (
            [nodes_result, edges_result]
            | "Flatten results" >> beam.Flatten()
            | "Compute final results" >> beam.CombineGlobally(sum_results)
            | "Override result kind" >> beam.Map(
                lambda r: Neo4jResult(r.count, r.nbytes, "final"))
            | "Echo final results" >> beam.ParDo(Echo(INFO, "final results:"))
        )
    logging.info(f"Finished creating graph '{g.name}'.")


if __name__ == "__main__":
    from os import environ
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    # General Parameters
    parser.add_argument(
        "--graph_json",
        type=str,
        required=True,
        help="Path to a JSON representation of the Graph model.",
    )
    parser.add_argument(
        "--mode",
        default=environ.get("DEFAULT_PIPELINE_MODE", "gcs"), # see Dockerfile
        help="Pipeline mode (if running via cli)",
        type=lambda x: str(x).lower(),
        choices=["gcs", "bigquery"],
    )

    # Neo4j Paramters
    parser.add_argument(
        "--neo4j_host",
        help="Hostname or IP address of Neo4j server.",
        default="localhost",
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
        type=util.strtobool,
        help="Use TLS for encrypting Neo4j Arrow Flight connection.",
    )
    parser.add_argument(
        "--neo4j_user",
        default="neo4j",
        help="Neo4j Username.",
    )
    parser.add_argument(
        "--neo4j_password",
        help="Neo4j Password",
    )
    parser.add_argument(
        "--neo4j_concurrency",
        default=4,
        type=int,
        help="Neo4j server-side concurrency.",
    )

    # GCS Parameters
    parser.add_argument(
        "--gcs_node_pattern",
        type=str,
        help="GCS URI file pattern to node parquet files. (Requires mode:gcs)",
    )
    parser.add_argument(
        "--gcs_edge_pattern",
        type=str,
        help="GCS URI file pattern to edge parquet files. (Requires mode:gcs)",
    )

    # BigQuery Parameters
    parser.add_argument(
        "--node_tables",
        help=(
            "Comma-separated list of BigQuery tables for nodes "
            "(Requires mode:bigquery)"
        ),
        type=lambda x: [y.strip() for y in str(x).split(",")],
        default=[],
    )
    parser.add_argument(
        "--edge_tables",
        help=(
            "Comma-separated list of BigQuery tables for edges "
            "(Requires mode:bigquery)"
        ),
        type=lambda x: [y.strip() for y in str(x).split(",")],
        default=[],
    )
    parser.add_argument(
        "--bq_project",
        type=str,
        help="GCP project containing BigQuery tables."
    )
    parser.add_argument(
        "--bq_dataset",
        type=str,
        help="BigQuery dataset containing BigQuery tables."
    )
    parser.add_argument(
        "--bq_max_stream_count",
        default=8192*2,
        type=int,
        help="Maximum number of streams to generate for a BigQuery table."
    )

    # Optional/Other Parameters
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose (debug) logging.",
    )

    args, beam_args = parser.parse_known_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(f"starting with args: {args}")

    # Make rocket go now...
    graph = load_model_from_path(args.graph_json)
    if not graph:
        graph = load_model_from_gcs(args.graph_json)
    if not graph:
        raise Exception(f"cannot load graph from {graph}")

    client = Neo4jArrowClient(args.neo4j_host, graph.name,
                              port=args.neo4j_port, tls=args.neo4j_use_tls,
                              database=graph.db, user=args.neo4j_user,
                              password=args.neo4j_password,
                              concurrency=args.neo4j_concurrency)
    if args.mode == "gcs":
        ### GCS!
        nodes, edges = args.gcs_node_pattern, args.gcs_edge_pattern
        if not nodes or not edges:
            raise Exception("missing valid nodes or edges pattern!")
        run_gcs_pipeline(graph, client, nodes, edges, beam_args)
    elif args.mode == "bigquery":
        ### BigQuery!
        project, dataset = args.bq_project, args.bq_dataset
        if not project or not dataset:
            raise Exception("you must set bq_project and bq_dataset to use "
                            "the BigQuery pipeline")
        nodes, edges = args.node_tables, args.edge_tables
        if not nodes or not edges:
            raise Exception("you must provide both nodes and edge table names")
        bq = BigQuerySource(project, dataset,
                            max_stream_count=args.bq_max_stream_count)

        # XXX
        node_streams = get_streams(bq, nodes)
        edge_streams = get_streams(bq, edges)
        run_bigquery_pipeline(graph, client, node_streams, edge_streams,
                              bq.copy(), beam_args)
    else:
        ### OH NO!
        raise Exception(f"invalid mode: {args.mode}")
