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
    def __init__(self, project_id: str, dataset: str, *,
                 max_stream_count: int = 8_192):
        self.project_id = project_id
        self.dataset = dataset
        self.client: Optional[BigQueryReadClient] = None
        self.basepath = f"projects/{self.project_id}/datasets/{self.dataset}"
        self.max_stream_count = max_stream_count

    def __str__(self):
        return f"BigQuerySource{{project_id={project_id}, dataset={dataset}}}"

    def table(self, table:str, *, fields: List[str] = []) -> List[str]:
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
            max_stream_count=self.max_stream_count,
        )
        return [stream.name for stream in session.streams]

    def consume_stream(self, stream: str) -> Arrow:
        """Apply consumer to a stream in the form of a generator"""
        if self.client is None:
            self.client = BigQueryReadClient()

        reader = self.client.read_rows(stream)
        rows = reader.rows()
        return rows.to_arrow()
