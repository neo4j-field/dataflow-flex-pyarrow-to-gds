# Dataflow Flex-Template for Neo4j GDS (via Apache Arrow)

> _"Don't ever go with the flow, be the flow..."_
>                      -- Jay Z, https://youtu.be/LceBAK8wyoc

This project contains pipelines and code for building Google Dataflow Flex
Templates that load data into Neo4j's in-memory Graph from:

- Apache Parquet files in Google Cloud Storage
- BigQuery tables

> **Goal**: provide a _no-code, cloud-native_ integration solution for data
> scientists and data engineers to bootstrap Neo4j Graph projections at scale.

## Requirements
Depending on if you're just consuming the template or looking to hack on it,
you'll need some or all of the following.

### Runtime
You'll need an environment with the following Neo4j products:
* `Neo4j v4.4.x` Enterprise Edition
* `Neo4j GDS v2.1` + Enterprise License

### Development
To build and deploy, you'll need the following tooling:

* `gcloud` (authenticated Google Cloud SDK tooling)
* `make` (tested with GNU Make)
* `python3`
* `pip`
* `mypy` (optional)

Run `$ pip install -r requirements` and optionally:

- `$ pip install mypy` -- for type checking
- `$ pip install pytest` -- for unit testing

The provided `Makefile` contains targets for both: `make mypy` & `make test`.

## Example Usage
The Makefile supports 3 different lifecycle options that chain together:

1. `make image-gcs` or `make image-bigquery`  -- Building the Docker images
2. `make build-gcs` or `make build-bigquery` -- Building the Dataflow Flex
   Template json files
3. `make run-gcs` or `make run-bigquery` -- Running a job using the Flex
   Template


### Building the Images
Two images are currently supported:

- `Dockerfile.gcs` -- builds an image for the GCS-based template
- `Dockerfile.bigquery` -- builds an image for the BigQuery-based template

To validate the `Dockerfile` logic, you can run `make image-gcs` or
`make image-bigquery` and it will use the Google Cloud Build service to create
and store an image in Google Container Registry.

> If you add files to the project, you may need to update the `Dockerfile`s and
> possibly the `Makefile`


### Building the Templates
To build and deploy a template file (to a GCS bucket), run `make build-gcs` or
`make build-bigquery` while providing the following parameters:

- `TEMPLATE_URI` -- a GCS uri that points to the location for the Google Flex
  Template builder's output.

Example:

```
# Build a template for GCS integration
$ make build-gcs TEMPLATE_URI="gs://my-bucket/template_gcs.json"

# Build a template for BigQuery integration
$ make build-bigquery TEMPLATE_URI="gs://my_bucket/template_bq.json"
```

> Note: `make build-gcs` and `make build-bigquery` will trigger the appropriate
> `make image-{gcs,bigquery}` task, so no need to run that step manually.


## The Graph Model
The template uses a graph model, constructed programmatically or via JSON, to
dictate how to translate the datasource fields to the appropriate parts (nodes,
edges) of the intended graph.

In Python, it looks like:

```python
import neo4j_arrow
from neo4j_arrow.model import Graph

G = (
    Graph(name="test", db="neo4j")
    .with_node(Node(source="gs://.*/papers.*parquet", label_field="labels",
                    key_field="paper"))
    .with_node(Node(source="gs://.*/authors.*parquet", label_field="labels",
                    key_field="author"))
    .with_node(Node(source="gs://.*/institution.*parquet", label_field="labels",
                    key_field="institution"))
    .with_edge(Edge(source="gs://.*/citations.*parquet", type_field="type",
                    source_field="source", target_field="target"))
    .with_edge(Edge(source="gs://.*/affiliation.*parquet", type_field="type",
                    source_field="author", target_field="institution"))
    .with_edge(Edge(source="gs://.*/authorship.*parquet", type_field="type",
                    source_field="author", target_field="paper"))
)
```

The same graph model, but in JSON:

```json
{
  "name": "test",
  "db": "neo4j",
  "nodes": [
    {
      "source": "gs://.*/papers.*parquet",
      "label_field": "labels",
      "key_field": "paper"
    },
    {
      "source": "gs://.*/authors.*parquet",
      "label_field": "labels",
      "key_field": "author"
    },
    {
      "source": "gs://.*/institution.*parquet",
      "label_field": "labels",
      "key_field": "institution"
    }
  ],
  "edges": [
    {
      "source": "gs://.*/citations.*parquet",
      "type_field": "type",
      "source_field": "source",
      "target_field": "target"
    },
    {
      "source": "gs://.*/affiliation.*parquet",
      "type_field": "type",
      "source_field": "author",
      "target_field": "institution"
    },
    {
      "source": "gs://.*/authorship.*parquet",
      "type_field": "type",
      "source_field": "author",
      "target_field": "paper"
    }
  ]
}
```

Currently, the JSON file can be provided locally (from a filesystem accessible
to the Apache Beam workers) or via a Google Cloud Storage uri (e.g. `gs://...`).

The fields of the Nodes and Edges in the  model have specific purposes:

- `source` -- a regex pattern used to match source data against the model, i.e.
  it's used to determine which node or edge a record corresponds to.
- `label_field` -- the source field name containing the node label or labels
- `key_field` -- the source field name containing the unique node identifier
  (NB: this currently must be a numeric field as of GDS 2.1.)
- `type_field` -- the source field name containing the edge type
- `source_field` -- the source field name containing the node identifier of the
  origin of an edge.
- `target_field` -- the source field name containing the node identifier of the
  target of an edge.

Other undocumented fields are supported, but not used as of yet.

Example model JSON files are provided in [example_models](./example_models).

## Running a Flex Template Job
To run a job from a template without trudging through the Google Cloud web
console, you can either run the `pipeline.py` directly (invoking the Apache Beam
DirectRunner) or use `make run` to deploy a Google DataFlow job.

Similar to `make build`, `make run` has some required and some optional
parameters:

#### Required Paramaters
- `GRAPH_JSON` -- path (local or GCS) of the Graph data model json file
- `NODES` -- GCS uri pattern or comma-separated list of BigQuery table names for
  nodes.
- `EDGES` -- GCS uri pattern or comma-separated list of BigQuery table names for
  edges.
- `REGION` -- GCP region to run the Dataflow job.
- `PROJECT` -- GCP project hosting BigQuery dataset. *(BigQuery only.)*
- `DATASET` -- BigQuery dataset name. *(BigQuery only.)*

#### Optional Parameters
- `JOBNAME` -- name of the Dataflow job (default is based on timestamp)
- `MAX_WORKERS` -- max number of workers to use at full scale (default: 8)
- `NUM_WORKERS` -- initial starting size of the worker pool (default: 4)
- `NEO4J_PORT` -- TCP port for the Neo4j GDS Arrow Service (default: 8491)
- `NE40J_TLS` -- Should we use TLS to connect to Neo4j? (default: True)
- `NEO4J_USER` -- username of Neo4j account (default: neo4j)
- `NEO4J_PASSWORD` -- password of Neo4j account (default: password)
- `NEO4J_DATABASE` -- owning database of the resulting graph (default: neo4j)
- `NEO4J_CONC` -- number of concurrent Arrow server-side threads (default: 4)

> Note: Parameters with `NEO4J_` prefix influence Neo4j features, not GCP.

## Examples
Let's look at the three different ways to use the template to run a job:

1. Locally using the `DirectRunner`
2. On Dataflow, submitted via the cli (gcloud)
3. On Dataflow, submitted via the web gui

### Local DirectRunner Invocation
Create a local virtual environment and `pip install -r requirements`. Ideally
you can run this on a virtual machine in GCE and can leverage some passive
authentication with a service account. (If not, google how to set up auth!)

The `pipeline.py` entrypoint supports **both** GCS and BigQuery pipelines. To
toggle between them, use the `--mode` parameter or set `DEFAULT_PIPELINE_MODE`
environment variable to either `gcs` or `bigquery`. If neither are specified,
the default behavior is to run in `gcs` mode.

Via the command line, not all args will populate with defaults. An example
invocation of the GCS mode via a shell on the Neo4j host:

```
$ python pipeline.py \
    --mode gcs \
    --neo4j_host localhost \
    --neo4j_user neo4j \
    --neo4j_password password \
    --graph_json gs://mybucket/graph.json \
    --gcs_node_pattern "gs://mybucket/nodes/**" \
    --gcs_edge_pattern "gs://mybucket/gcdemo/edges/**"
    --neo4j_use_tls False \
    --neo4j_concurrency 32
```

### Dataflow Job Invocation
Assuming you've built a template (`make build-gcs`), here's an example of
submitting a GCS job via the provided `Makefile`:

```
$ make run-gcs \
    REGION=us-central1 \
    TEMPLATE_URI=gs://neo4j_voutila/gcdemo/template_gcs.json \
    NEO4J_HOST=some-hostname.us-central1-c.c.some-gcpproject.internal \
    GRAPH_JSON=./test.json \
    GCS_NODES="gs://my_bucket/nodes/**" \
    GCS_EDGES="gs://my_bucket/edges/**" \
    NEO4J_TLS=False
```

Behind the scenes, it's invoking `gcloud dataflow-flex-template run` and passing
in the appropriate parameters. Output is pumped through `awk(1)` so you'll also
get a handly little url to pop into your browser to watch the job status in the
GCP console. ;-)


### Dataflow Job via Web GUI
To start, using the GCP web console requires having run `make build`.

1. Navigate to `Pipelines` in the `Dataflow` console.
2. Create a new data pipeline.
3. For the _Dataflow template", select `Custom Template` (way at the bottom).
4. Browse to the GCS location of your template. This should be what you set the
   `TEMPLATE_URI` parameter to when running `make build`.
5. Select `Batch` for the _Pipeline type_.
6. Make sure to expand _Show Optional Paramters_ and populate as needed.

> Note: The above steps were accurate as of 22 June 2022. The GCP console may
> have changed since this was written.

## Currently Known Caveats
This is a work in progress. Expect some bumps along the way:

- Performance tuning not done yet.
  + BigQuery pipeline could use some work to get better throughput.
  + GCS pipeline seems ok, but hasn't been analyzed.
- Some caveats exist in the GDS Arrow Flight server. See the
  [docs](https://neo4j.com/docs/graph-data-science/current/graph-project-apache-arrow/)
  for details.
- Logging can be improved.
- Template metadata regex is _super_ lax.

If something is horribly broken, please open an issue.

## Contributing
See the [backlog](./TODO.md) file for ideas of where you can help.

If you are not a Neo4j employee or contractor, you may be required to agree to
the terms of the [Neo4j CLA](https://neo4j.com/developer/cla/) before we can
accept your contributions.


## License & Copyright

The works provided are copyright 2022 Neo4j, Inc.

All files in this project are made available under the Apache License, Version
2.0 (see [LICENSE](./LICENSE)) unless otherwise noted. If/when there are
exceptions, the applicable license and copyright will be noted within the
individual file.
