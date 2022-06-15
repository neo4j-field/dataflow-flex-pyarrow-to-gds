# todo's

Looking to help? Here's a backlog to shovel...

- [ ] Add a pipeline (and new image) that loads from named BigQuery tables.
- [ ] Adjust the Parquet file pipeline to take more flexible GCS patterns.
- [ ] Move neo4j password/token to GCP Secrets Manager, replacing the
      `neo4j_password` parameter with the identifier for a secret.
- [ ] Figure out better logging to Dataflow from the Python code.
- [ ] Provide metrics (e.g. nodes loaded) to the Dataflow Runner.
