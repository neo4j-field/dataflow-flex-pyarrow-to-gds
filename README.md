# Dataflow Flex-Template for Neo4j GDS (via Apache Arrow)

> _"Don't ever go with the flow, be the flow..."_
>                      -- Jay Z, https://youtu.be/LceBAK8wyoc

## Requirements
* `Neo4j v4.4.x` Enterprise Edition
* `Neo4j GDS v2.1` + Enterprise License
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

## Example

Assuming you've built a template, here's an example of submitting a job via the
provided makefile:

```
$ make run \
    REGION=us-central1 \
    TEMPLATE_URI=gs://neo4j_voutila/gcdemo/template.json \
    NEO4J_HOST=some-hostname.us-central1-c.c.some-gcpproject.internal \
    NEO4J_GRAPH=test \
    GCS_NODES="gs://my_bucket/nodes/**" \
    GCS_EDGES="gs://my_bucket/edges/**" \
    NEO4J_TLS=False
```

## Contributing

See the [backlog](./TODO.md) file for ideas of where you can help.

If you are not a Neo4j employee or contractor, you may be required to agree to
the terms of the [Neo4j CLA](https://neo4j.com/developer/cla/) before we can
accept your contributions.


## License & Copyright

The works provided are copyright 2022 Neo4j, Inc.

All files in this project are made available under the Apache License, Version
2.0 (see [LICENSE](./LICENSE)] unless otherwise noted. If/when there are
exceptions, the applicable license and copyright will be noted within the
individual file.
