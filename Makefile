VERSION		=	"0.0.1-preview"
IMAGE		!=	gcloud config get project
TAG		:=	"gcr.io/${IMAGE}/neo4j-dataflow-flex-gds:${VERSION}"
TEMPLATE_URI	:=	"gs://updatemedude"

# Default target to help check settings
info:
	echo "VERSION: ${VERSION}"
	echo "IMAGE: ${IMAGE}"
	echo "TAG: ${TAG}"
	echo "TEMPLATE_URI: ${TEMPLATE_URI}"

# Builds & submits the Dockerfile to Google Cloud Registry
image: Dockerfile
	gcloud builds submit --tag "${TAG}"

# Generate the Dataflow Flex-Template, storing at $TEMPLATE_URI
build: image
	gcloud dataflow flex-template build "${TEMPLATE_URI}" \
		--image "${IMAGE}" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata.json"

.PHONY:	image info build
.SILENT: info
