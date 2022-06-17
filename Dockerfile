FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG PIPELINE_PY=parquet_in_gcs.py
ARG WORKDIR=/dataflow/template

RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Note: Do not include `apache-beam` in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/${PIPELINE_PY}"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

# Install apache-beam and other dependencies
COPY requirements.txt .
RUN pip install -qq apache-beam[gcp] \
	pip install -qq -r ./requirements.txt # still use this for now

# Copy in our base files
COPY neo4j_arrow ./neo4j_arrow
COPY neo4j_beam ./neo4j_beam
COPY setup.py .

# Copy in our pipeline file
COPY ${PIPELINE_PY} .
