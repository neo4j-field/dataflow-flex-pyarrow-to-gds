FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Note: Do not include `apache-beam` in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/neo4j_gds_beam.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

# Install apache-beam and other dependencies
COPY requirements.txt .
RUN pip install -qq apache-beam[gcp] \
	pip install -qq -r ./requirements.txt # still use this for now

# Copy in our files
COPY neo4j_arrow ./neo4j_arrow
COPY setup.py .
COPY neo4j_gds_beam.py .
