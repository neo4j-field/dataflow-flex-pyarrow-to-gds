FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY requirements.txt .
COPY neo4j_arrow.py .
COPY neo4j_gds_beam.py .

# Do not include `apache-beam` in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/neo4j_gds_beam.py"

# Install apache-beam and other dependencies to launch the pipeline
RUN pip install -qq -U pip wheel
RUN pip install -qq apache-beam[gcp]
RUN pip install -qq -r ./requirements.txt
