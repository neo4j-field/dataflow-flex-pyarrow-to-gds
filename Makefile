VERSION		=	0.1.0
IMAGE		!=	gcloud config get project
TAG		:=	gcr.io/${IMAGE}/neo4j-dataflow-flex-gds:${VERSION}
TEMPLATE_URI	:=	gs://updatemedude
TIMESTAMP	!=	date -u "+%Y%m%d-%H%M%S"
MYPYBIN		!=	command -v mypy

# Optional for `run` target (most match defaults):
JOBNAME		:=	dataflow-pyarrow-neo4j-${TIMESTAMP}
NEO4J_HOST	:=	localhost
NEO4J_PORT	:=	8491
NEO4J_TLS	:=	True
NEO4J_USER	:=	neo4j
NEO4J_PASSWORD	:=	password
NEO4J_GRAPH	:=	graph
NEO4J_DATABASE	:=	neo4j
NEO4J_CONC	:=	4
GCS_NODES	:=	gs://neo4j_voutila/gcdemo/nodes/papers/**
GCS_EDGES	:=	gs://neo4j_voutila/gcdemo/edges/citations/**
REGION		:=	us-central1
MAX_WORKERS	:=	4


# Default target to help check settings
info:
	echo "VERSION: ${VERSION}"
	echo "IMAGE: ${IMAGE}"
	echo "TAG: ${TAG}"
	echo "TEMPLATE_URI: ${TEMPLATE_URI}"
	echo "TIMESTAMP: ${TIMESTAMP}"
	echo "MYPYBIN ${MYPYBIN}"

validate:
ifneq (${TEMPLATE_URI},)
	echo "No TEMPLATE_URI provided. Please set it!" >&2
	false
else
	true
endif

# If we have mypy (pip install mypy), check our python files for issues first.
mypy:
ifneq (${MYPYBIN},)
	mypy neo4j_gds_beam.py
	mypy neo4j_arrow
else
	echo "no mypy, skipping type checking"
endif

# Builds & submits the Dockerfile to Google Cloud Registry
image: Dockerfile mypy
	gcloud builds submit --tag "${TAG}"

# Generate the Dataflow Flex-Template, storing at $TEMPLATE_URI
build: image
	gcloud dataflow flex-template build "${TEMPLATE_URI}" \
		--image "${TAG}" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata.json"

# Run this puppy
run:
	gcloud dataflow flex-template run "${JOBNAME}" \
		--template-file-gcs-location "${TEMPLATE_URI}" \
		--region "${REGION}" \
		--max-workers "${MAX_WORKERS}" \
		--parameters neo4j_host="${NEO4J_HOST}" \
		--parameters neo4j_port="${NEO4J_PORT}" \
		--parameters neo4j_use_tls="${NEO4J_TLS}" \
		--parameters neo4j_user="${NEO4J_USER}" \
		--parameters neo4j_password="${NEO4J_PASSWORD}" \
		--parameters neo4j_graph="${NEO4J_GRAPH}" \
		--parameters neo4j_concurrency="${NEO4J_CONC}" \
		--parameters gcs_node_pattern="${GCS_NODES}" \
		--parameters gcs_edge_pattern="${GCS_EDGES}"


.PHONY:	build image info mypy run
.SILENT: info
