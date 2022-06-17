# todo's

Looking to help? Here's a backlog to shovel...

- [ ] Fixup the regex for the template parameters. (They're super lax.)
- [ ] Add a pipeline (and new image) that loads from named BigQuery tables.
- [ ] Adjust the Parquet file pipeline to take more flexible GCS patterns.
- [ ] Move neo4j password/token to GCP Secrets Manager, replacing the
      `neo4j_password` parameter with the identifier for a secret.
- [ ] Figure out better logging to Dataflow from the Python code.
- [ ] Provide metrics (e.g. nodes loaded) to the Dataflow Runner.
- [x] ~"Vendor in" the `distutils.util.strtobool` function before Python 3.12~
      ~removes the `distutils` module.~

Please open a PR and assign or notify `@voutilad`.
