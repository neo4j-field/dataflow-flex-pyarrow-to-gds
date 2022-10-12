from setuptools import find_packages, setup

setup(
    name="dataflow-pyarrow-neo4j",
    version="0.7.0",

    url="https://github.com/neo4j-field/dataflow-flex-pyarrow-to-gds",
    maintainer="Dave Voutila",
    maintainer_email="dave.voutila@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[
        "neo4j_arrow @ git+https://github.com/neo4j-field/neo4j_arrow@0.2.0#egg=neo4j_arrow",
        "google-cloud-bigquery-storage[pyarrow]",
    ],
    packages=find_packages(),
)
