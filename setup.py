from setuptools import find_packages, setup

setup(
    name="dataflow-pyarrow-neo4j",
    version="0.4.0",

    url="https://github.com/neo4j-field/dataflow-flex-pyarrow-to-gds",
    maintainer="Dave Voutila",
    maintainer_email="dave.voutila@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[
        "pyarrow==7.0.0",
        "google-cloud-bigquery-storage",
    ],
    packages=find_packages(),
)
