VERSION	=	"0.0.1-preview"
IMAGE	!=	gcloud config get project
TAG	:=	"gcr.io/${IMAGE}/neo4j-dataflow-flex-gds:${VERSION}"

info:
	echo "Version: ${VERSION}"
	echo "Image: ${IMAGE}"
	echo "Tag: ${TAG}"

image:
	gcloud builds submit --tag "${TAG}"

build: image
	gcloud dataflow flex-template build . \
		--image "${IMAGE}" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata.json"

.PHONY:	image info build
.SILENT: info
