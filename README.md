# Dataflow Flex-Template for Neo4j GDS (via Apache Arrow)



## Requirements

* `gcloud` (authenticated Google Cloud SDK tooling)
* `make` (tested with GNU Make)
* `mypy` (optional)

## Example Usage
The Makefile supports 3 different lifecycle options that chain together:

1. `make image` -- Building the Docker image
2. `make build` -- Building the Dataflow Flex Template json file
3. `make run`   -- Running a job using the Flex Template

### Building the Image
To validate the `Dockerfile` logic, you can run `make image` and it will use the
Google Cloud Build service to create and store an image in Google Container
Registry.

> If you add files to the project, you may need to update the `Dockerfile`

### Building the Template
To build and deploy the template file (to a GCS bucket), run `make build`.

You must provide a `TEMPLATE_URI` (a GCS uri) that points to the location for
the Google Flex Template builder's output. You can pass this as an argument to
make.

```
$ make build TEMPLATE_URI="gs://my-bucket/my-template.json"
```

> Note: `make build` will trigger `make image`.

### Running a Flex Template Job
To run a job a built template without trudging through the Google Cloud web
console, you can run `make run` and provide one or many of the following runtime
options:

> Note: parameters with `NEO4J_` prefix influence Neo4j features, not GCP.

#### Required Paramaters
- `NEO4J_HOST` -- hostname of ip address of the target Neo4j server
- `NEO4J_GRAPH` -- name of the resulting Neo4j GDS Graph
- `GCS_NODES` -- GCS uri pattern to the parquet files representing nodes
- `GCS_EDGES` -- GCS uri pattern to the parquet files representing edges
- `REGION` -- GCP region to run the Dataflow job

#### Optional Parameters
- `JOBNAME` -- name of the Dataflow job (default is based on timestamp)
- `MAX_WORKERS` -- max number of workers to use at full scale (default: 4)
- `NEO4J_PORT` -- TCP port for the Neo4j GDS Arrow Service (default: 8491)
- `NE40J_TLS` -- Should we use TLS to connect to Neo4j? (default: True)
- `NEO4J_USER` -- username of Neo4j account (default: neo4j)
- `NEO4J_PASSWORD` -- password of Neo4j account (default: password)
- `NEO4J_DATABASE` -- owning database of the resulting graph (default: neo4j)
- `NEO4J_CONC` -- number of concurrent Arrow server-side threads (default: 4)

## Contributing

See the [backlog](./TODO.md) file for ideas of where you can help.
