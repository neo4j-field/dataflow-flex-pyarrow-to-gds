from setuptools import find_packages, setup

setup(
    name="dataflow-pyarrow-neo4j",
    version="0.8.1",

    url="https://github.com/neo4j-field/dataflow-flex-pyarrow-to-gds",
    maintainer="Dave Voutila",
    maintainer_email="dave.voutila@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[
        "neo4j_arrow @ https://github.com/neo4j-field/neo4j_arrow/archive/refs/tags/0.3.0.tar.gz",
        "google-cloud-bigquery-storage[pyarrow]",
    ],
    packages=find_packages(),
)
