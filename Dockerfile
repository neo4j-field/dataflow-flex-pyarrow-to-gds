FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Do not include `apache-beam` in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/neo4j_gds_beam.py"

# Install apache-beam
RUN pip install -qq -U pip wheel
RUN pip install -qq apache-beam[gcp]

# Install 3rd party dependencies
COPY requirements.txt .
RUN pip install -qq -r ./requirements.txt

# Copy in our files
COPY neo4j_arrow.py .
COPY neo4j_gds_beam.py .
