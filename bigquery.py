#!/usr/bin/env python3
import argparse
import logging
from logging import INFO, DEBUG

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq
from apache_beam.options.pipeline_options import PipelineOptions

import pyarrow as pa

from neo4j_arrow import Neo4jArrowClient
from neo4j_arrow.model import Graph, Node, Edge

import neo4j_beam as nb
from neo4j_beam import (
    CopyKeyToMetadata, Echo, Neo4jResult, WriteEdges, WriteNodes, Signal,
    sum_results
)

from neo4j_bigquery import BigQuerySource

from typing import cast, List, Generator, Optional, Tuple, Union


Arrow = Union[pa.Table, pa.RecordBatch]
StreamKey = Tuple[str, int]
TupleStream = Generator[Tuple[StreamKey, str], None, None]
KeyedArrowStream = Generator[Tuple[StreamKey, Arrow], None, None]

G = (
    Graph(name="test", db="neo4j")
    .with_node(Node(source="papers", label_field="labels",
                    key_field="paper"))
    .with_node(Node(source="authors", label_field="labels",
                    key_field="author"))
    .with_node(Node(source="institution", label_field="labels",
                    key_field="institution"))
    .with_edge(Edge(source="citations", type_field="type",
                    source_field="source", target_field="target"))
    .with_edge(Edge(source="affiliation", type_field="type",
                    source_field="author", target_field="institution"))
    .with_edge(Edge(source="authorship", type_field="type",
                    source_field="author", target_field="paper"))
)


def load_model_from_path(path: str) -> Optional[Graph]:
    try:
        with open(path, mode='r') as f:
            lines = f.readlines()
            return Graph.from_json(''.join(lines))
    except Exception:
        logging.info(f"not a local file: {path}")
        return None


def load_model_from_gcs(uri: str) -> Optional[Graph]:
    try:
        import google
        from google.cloud import storage
        from google.cloud.storage.blob import Blob

        gcs = storage.Client()
        blob = Blob.from_string(uri, client=gcs)
        payload = blob.download_as_text()
        return Graph.from_json(payload)
    except Exception as e:
        logging.info(f"could not load from uri: {uri}")
        logging.info(f"error: {e}")
        return None


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
        arrow = self.bq_source.consume_stream(stream)
        schema = arrow.schema.with_metadata({"src": table})
        arrow = arrow.from_arrays(arrow.columns, schema=schema)
        logging.info(f"ReadBQStream: got arrow obj w/ {arrow.num_rows:,} rows")
        yield (key, arrow)


def run(host: str, port: int, user: str, password: str, tls: bool,
        concurrency: int, gcs_node_pattern: str, gcs_edge_pattern: str,
        graph_json: str, beam_args: List[str] = None) -> None:
    """It's Morbin time!"""
    options = PipelineOptions(beam_args, save_main_session=True)
    global G
    if graph_json:
        g = load_model_from_path(graph_json)
        if not g:
            g = load_model_from_gcs(graph_json)
        assert G is not None
        G = cast(Graph, g)
    client = Neo4jArrowClient(host, G.name, port=port, user=user,
                              password=password, tls=tls, database=G.db,
                              concurrency=concurrency)
    logging.info(f"Using graph model: {G}")

    bq = BigQuerySource("neo4j-se-team-201905", "gcdemo", max_stream_count=2048)

    logging.info(f"Starting job with {gcs_node_pattern} and {gcs_edge_pattern}")
    # "The Joys of Beam"
    with beam.Pipeline(options=options) as pipeline:
        client.start()
        node_result = (
            pipeline
            | "Begin loading Node tables" >> beam.Create([
                "authors", "papers", "institution"
            ])
            | "Discover node streams" >> beam.ParDo(GetBQStream(bq))
            | "Read node BQ streams" >> beam.ParDo(ReadBQStream(bq))
            #| "Copy node keys" >> beam.ParDo(CopyKeyToMetadata(
            #    metadata_field="src"))
            | "Send nodes to Neo4j" >> beam.ParDo(WriteNodes(client, G, "src"))
            | "Sum node results" >> beam.CombineGlobally(sum_results)
            | "Echo node results" >> beam.ParDo(Echo(INFO, "node result:"))
        )
        nodes_done = (
            node_result
            | "Signal node completion" >> beam.ParDo(
                nb.Signal(client, "nodes_done",
                          ["citations", "authorship", "affiliation"]))
        )
        edge_result = (
            nodes_done
            | "Discover edge streams" >> beam.ParDo(GetBQStream(bq))
            | "Read Edge BQ streams" >> beam.ParDo(ReadBQStream(bq))
            #| "Copy edge keys" >> beam.ParDo(CopyKeyToMetadata(
            #    metadata_field="src"))
            | "Send edges to Neo4j" >> beam.ParDo(WriteEdges(client, G, "src"))
            | "Sum edge results" >> beam.CombineGlobally(sum_results)
            | "Echo edge results" >> beam.ParDo(Echo(INFO, "edge result:"))
            | "Signal edge completion" >> beam.ParDo(Signal(client, "edges_done"))
        )
        results = (
            [node_result, edge_result]
            | "Flatten results" >> beam.Flatten()
            | "Compute final results" >> beam.CombineGlobally(sum_results)
            | "Override result kind" >> beam.Map(
                lambda r: Neo4jResult(r.count, r.nbytes, "final"))
            | "Echo final results" >> beam.ParDo(Echo(INFO, "final results:"))
        )
    logging.info(f"Finished creating graph '{G.name}'.")


if __name__ == "__main__":
    # TODO: env toggle for log level?
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        default="False",
        type=nb.util.strtobool,
        help="Enable debug logging.",
    )
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

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Make rocket go now...
    run(args.neo4j_host, args.neo4j_port, args.neo4j_user, args.neo4j_password,
        args.neo4j_use_tls, args.neo4j_concurrency, args.gcs_node_pattern,
        args.gcs_edge_pattern, args.graph_json, beam_args)
