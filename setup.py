from setuptools import find_packages, setup

setup(
    name="api_to_s3",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-aws",
        "dagster-cloud",
        "pandas",
        "pyarrow",
        "boto3",
        "dagster-webserver"
    ]
)
