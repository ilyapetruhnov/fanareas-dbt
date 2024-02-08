from setuptools import find_packages, setup

setup(
    name="dagster_fanareas",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dagster_fanareas": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-postgres",
        # "dbt-duckdb",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)

