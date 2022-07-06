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
                 max_stream_count: int = 1_000):
        self.project_id = project_id
        self.dataset = dataset
        self.client: Optional[BigQueryReadClient] = None
        self.basepath = f"projects/{self.project_id}/datasets/{self.dataset}"
        if max_stream_count < 1:
            raise ValueError("max_stream_count must be greater than 0")
        self.max_stream_count = min(1_000, max_stream_count)

    def __str__(self):
        return f"BigQuerySource{{{self.basepath}}}"

    def __getstate__(self):
        state = self.__dict__.copy()
        if "client" in state:
            del state["client"]
        return state

    def copy(self) -> "BigQuerySource":
        source = BigQuerySource(self.project_id, self.dataset,
                                max_stream_count=self.max_stream_count)
        return source

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

    def consume_stream(self, stream: str) -> ArrowStream:
        """Apply consumer to a stream in the form of a generator"""
        if getattr(self, "client", None) is None:
            self.client = BigQueryReadClient()

        reader = self.client.read_rows(stream)
        rows = reader.rows()
        for page in rows.pages:
            yield page.to_arrow()
