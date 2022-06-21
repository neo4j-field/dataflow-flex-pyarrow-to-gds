# Build environment parameters (only change if you are hacking on this project)
VERSION		=	0.4.0
IMAGE		!=	gcloud config get project
TAG		:=	gcr.io/${IMAGE}/neo4j-dataflow-flex-gds:${VERSION}
TIMESTAMP	!=	date -u "+%Y%m%d-%H%M%S"
MYPY		!=	command -v mypy
PYTEST		!=	command -v pytest
CONSOLE_BASE	=	https://console.cloud.google.com/dataflow/jobs

# Required parameters (must be populated on cli for main make targets)
GCS_NODES	:=
GCS_EDGES	:=
TEMPLATE_URI	:=
REGION		:=
NEO4J_HOST	:=
GRAPH_JSON	:=	./mag240.json

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

# Related to source files
PIPELINES	=	parquet_in_gcs.py bigquery.py
MODULES		=	neo4j_arrow neo4j_beam neo4j_bigquery


# Default target to help check settings
info:
	@echo "VERSION: ${VERSION}"
	@echo "IMAGE: ${IMAGE}"
	@echo "TAG: ${TAG}"
	@echo "TEMPLATE_URI: ${TEMPLATE_URI}"
	@echo "TIMESTAMP: ${TIMESTAMP}"
	@echo "MYPY: ${MYPY}"
	@echo "PYTEST: ${PYTEST}"
	@echo "GRAPH_JSON: ${GRAPH_JSON}"

validate-build:
ifeq (${TEMPLATE_URI},)
	@echo "No TEMPLATE_URI provided. Please set it!" >&2; false
else
	@true
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
	@${PYTEST}
else
	@echo "no pytest, skipping tests"
endif

# Builds & submits the Dockerfile to Google Cloud Registry
image: mypy test Dockerfile
	@gcloud builds submit --tag "${TAG}"

# Generate the Dataflow Flex-Template, storing at $TEMPLATE_URI
build: validate-build image
	@gcloud dataflow flex-template build "${TEMPLATE_URI}" \
		--image "${TAG}" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata.json"

validate-run:
ifeq (${REGION},)
	@echo "No REGION provided. Please set to a valid GCP region!" >&2; false
else ifeq (${NEO4J_HOST},)
	@echo "No NEO4J_HOST provided. Please set to a valid host or IP!" >&2
	@false
else ifeq (${GRAPH_JSON},)
	@echo "No GRAPH_JSON provided. Please set to a valid uri or path!" >&2;
	@false
else ifeq (${GCS_NODES},)
	@echo "No GCS_NODES provided. Please provide a GCS uri!" >&2; false
else ifeq (${GCS_EDGES},)
	@echo "No GCS_EDGES provided. Please provide a GCS uri!" >&2; false
else
	@true
endif

# Run this puppy
run: validate-run
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
		--parameters gcs_node_pattern="${GCS_NODES}" \
		--parameters gcs_edge_pattern="${GCS_EDGES}" \
	| awk ' BEGIN { jobId = ""; projId = ""; } \
		{ if ($$1 == "id:") { jobId = $$2; } \
		  if ($$1 == "projectId:") { projId = $$2; } \
		  print $$N; \
		} \
		END { if (jobId != "") \
		print "Console url: ${CONSOLE_BASE}/${REGION}/" \
			jobId "?project=" projId }'


.PHONY:	build image info mypy run test validate-build validate-run
.NOTPARALLEL: build image info mypy run validate-build validate-run
