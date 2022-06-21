import logging

from google.cloud.bigquery_storage import (
    BigQueryReadClient, DataFormat, ReadSession
)

import pyarrow as pa
import neo4j_arrow as na

from typing import Any, Dict, Generator, List, Optional, Union, Tuple


Arrow = Union[pa.Table, pa.RecordBatch]
ArrowStream = Generator[Arrow, None, None]


class BigQuerySource:
    """
    Wrapper around a BigQuery Dataset. Uses the Storage API to generate a list
    of streams that the BigQueryReadClient can fetch.
    """
    def __init__(self, project_id: str, dataset: str):
        self.project_id = project_id
        self.dataset = dataset
        self.client: Optional[BigQueryReadClient] = None
        self.basepath = f"projects/{self.project_id}/datasets/{self.dataset}"

    def __str__(self):
        return f"BigQuerySource{{project_id={project_id}, dataset={dataset}}}"

    def table(self, table:str, *, fields: List[str] = [],
              max_stream_count:int = 8_192 * 2) -> List[str]:
        """Get one or many Arrow-based streams for a given BigQuery table."""
        if self.client is None:
            self.client = BigQueryReadClient()

        read_session = ReadSession(
            table=f"{self.basepath}/tables/{table}",
            data_format=DataFormat.ARROW
        )
        if fields:
            read_session.read_options.selected_fields=fields

        session = self.client.create_read_session(
            parent=f"projects/{self.project_id}",
            read_session=read_session,
            max_stream_count=max_stream_count,
        )
        return [stream.name for stream in session.streams]

    def consume_stream(self, stream: str, **metadata) -> ArrowStream:
        """Apply consumer to a stream in the form of a generator"""
        if self.client is None:
            self.client = BigQueryReadClient()

        reader = self.client.read_rows(stream)
        rows = reader.rows()

        for page in rows.pages:
            arrow = page.to_arrow()
            schema = arrow.schema.with_metadata(metadata)
            arrow = arrow.from_arrays(arrow.columns, schema=schema)
            logging.info("BQ arrow: {arrow}")
            yield arrow
