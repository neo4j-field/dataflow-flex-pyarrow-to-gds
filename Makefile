# Build environment parameters (only change if you are hacking on this project)
VERSION		=	0.6.0
PROJECT		!=	gcloud config get project
TAG_GCS		:=	gcr.io/${PROJECT}/neo4j-gcs-to-gds:${VERSION}
TAG_BIGQUERY	:=	gcr.io/${PROJECT}/neo4j-bigquery-to-gds:${VERSION}
TIMESTAMP	!=	date -u "+%Y%m%d-%H%M%S"
MYPY		!=	command -v mypy
PYTEST		!=	command -v pytest
CONSOLE_BASE	=	https://console.cloud.google.com/dataflow/jobs

# Optional for `run` target (most match defaults):
JOBNAME		:=	dataflow-pyarrow-neo4j-${TIMESTAMP}
NUM_WORKERS	:=	4
MAX_WORKERS	:=	8
NEO4J_CONC	:=	8
NEO4J_DATABASE	:=	neo4j
NEO4J_PASSWORD	:=	password
NEO4J_PORT	:=	8491
NEO4J_TLS	:=	True
NEO4J_USER	:=	neo4j
DATASET		:=

# Required parameters (must be populated on cli for main make targets)
NODES		:=
EDGES		:=
TEMPLATE_URI	:=
REGION		:=
NEO4J_HOST	:=
GRAPH_JSON	:=

# Related to source files
PIPELINES	=	pipeline.py
MODULES		=	neo4j_arrow neo4j_beam neo4j_bigquery


# Default target to help check settings
info:
	@echo "VERSION: ${VERSION}"
	@echo "PROJECT: ${PROJECT}"
	@echo "TAG_GCS: ${TAG_GCS}"
	@echo "TAG_BIGQUERY: ${TAG_BIGQUERY}"
	@echo "TEMPLATE_URI: ${TEMPLATE_URI}"
	@echo "TIMESTAMP: ${TIMESTAMP}"
	@echo "MYPY: ${MYPY}"
	@echo "PYTEST: ${PYTEST}"
	@echo "GRAPH_JSON: ${GRAPH_JSON}"

validate-build:
ifeq (${TEMPLATE_URI},)
	@echo "No TEMPLATE_URI provided. Please set it!" >&2; false
else
	@echo ">>> Using TEMPLATE_URI=${TEMPLATE_URI}"
endif

# If we have mypy (pip install mypy), check our python files for issues first.
mypy:
ifneq (${MYPY},)
	@${MYPY} ${PIPELINES} ${MODULES}
else
	@echo "no mypy, skipping type checking"
endif

# Run pytest if we have it (pip install pytest)
test:
ifneq (${PYTEST},)
	@echo ">>> Running pytest..."
	@${PYTEST}
else
	@echo "no pytest, skipping tests"
endif

# Builds & submits the Dockerfile to Google Cloud Registry
image-gcs: mypy test Dockerfile.gcs
	@echo ">>> Building Docker image for GCS flex template..."
	@cp Dockerfile.gcs Dockerfile
	@gcloud builds submit --tag "${TAG_GCS}"
	@rm -f Dockerfile

image-bigquery: mypy test Dockerfile.bigquery
	@echo ">>> Building Docker image for BigQuery flex template..."
	@cp Dockerfile.bigquery Dockerfile
	@gcloud builds submit --tag "${TAG_BIGQUERY}"
	@rm -f Dockerfile

# Generate the Dataflow Flex-Template, storing at $TEMPLATE_URI
build-gcs: validate-build image-gcs
	@echo ">>> Building GCS flex template @ ${TEMPLATE_URI}..."
	@gcloud dataflow flex-template build "${TEMPLATE_URI}" \
		--image "${TAG_GCS}" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata_gcs.json"

build-bigquery: validate-build image-bigquery
	@echo ">>> Building BigQuery flex template @ ${TEMPLATE_URI}..."
	gcloud dataflow flex-template build "${TEMPLATE_URI}" \
		--image "${TAG_BIGQUERY}" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata_bq.json"

validate-run:
ifeq (${REGION},)
	@echo "No REGION provided. Please set to a valid GCP region!" >&2; false
else ifeq (${NEO4J_HOST},)
	@echo "No NEO4J_HOST provided. Please set to a valid host or IP!" >&2
	@false
else ifeq (${GRAPH_JSON},)
	@echo "No GRAPH_JSON provided. Please set to a valid uri or path!" >&2;
	@false
else ifeq (${NODES},)
	@echo "No NODES provided!" >&2; false
else ifeq (${EDGES},)
	@echo "No EDGES provided!" >&2; false
else
	@true
endif

# Run this puppy
run-gcs: validate-run
	@echo ">>> Running GCS flex template from ${TEMPLATE_URI}..."
	@gcloud dataflow flex-template run "${JOBNAME}" \
		--template-file-gcs-location "${TEMPLATE_URI}" \
		--region "${REGION}" \
		--num-workers "${NUM_WORKERS}" \
		--max-workers "${MAX_WORKERS}" \
		--parameters graph_json="${GRAPH_JSON}" \
		--parameters neo4j_host="${NEO4J_HOST}" \
		--parameters neo4j_port="${NEO4J_PORT}" \
		--parameters neo4j_use_tls="${NEO4J_TLS}" \
		--parameters neo4j_user="${NEO4J_USER}" \
		--parameters neo4j_password="${NEO4J_PASSWORD}" \
		--parameters neo4j_concurrency="${NEO4J_CONC}" \
		--parameters gcs_node_pattern="${NODES}" \
		--parameters gcs_edge_pattern="${EDGES}" \
	| awk ' BEGIN { jobId = ""; projId = ""; } \
		{ if ($$1 == "id:") { jobId = $$2; } \
		  if ($$1 == "projectId:") { projId = $$2; } \
		  print $$N; \
		} \
		END { if (jobId != "") \
		print "Console url: ${CONSOLE_BASE}/${REGION}/" \
			jobId "?project=" projId }'

run-bigquery: validate-run
	@echo ">>> Running BigQuery flex template from ${TEMPLATE_URI}..."
	@gcloud dataflow flex-template run "${JOBNAME}" \
		--template-file-gcs-location "${TEMPLATE_URI}" \
		--region "${REGION}" \
		--num-workers "${NUM_WORKERS}" \
		--max-workers "${MAX_WORKERS}" \
		--parameters graph_json="${GRAPH_JSON}" \
		--parameters neo4j_host="${NEO4J_HOST}" \
		--parameters neo4j_port="${NEO4J_PORT}" \
		--parameters neo4j_use_tls="${NEO4J_TLS}" \
		--parameters neo4j_user="${NEO4J_USER}" \
		--parameters neo4j_password="${NEO4J_PASSWORD}" \
		--parameters neo4j_concurrency="${NEO4J_CONC}" \
		--parameters bq_project="${PROJECT}" \
		--parameters bq_dataset="${DATASET}" \
		--parameters ^:^node_tables="${NODES}" \
		--parameters ^:^edge_tables="${EDGES}" \
	| awk ' BEGIN { jobId = ""; projId = ""; } \
		{ if ($$1 == "id:") { jobId = $$2; } \
		  if ($$1 == "projectId:") { projId = $$2; } \
		  print $$N; \
		} \
		END { if (jobId != "") \
		print "Console url: ${CONSOLE_BASE}/${REGION}/" \
			jobId "?project=" projId }'


.PHONY:	build-gcs build-bigquery image-gcs image-bigquery info mypy run-gcs
	run-bigquery test validate-build validate-run
.NOTPARALLEL: build-gcs build-bigquery image-gcs image-bigquery info mypy
	run-gcs run-bigquery test validate-build validate-run
