import os
from pathlib import Path

from dagster_dbt import DbtCliResource
from dagster import EnvVar
# from collections.abc import namedtuple
from dagster import EnvVar
import os
from urllib.parse import quote
# from dagster_postgres.utils import get_conn_string

# start_resources_marker_0
# DbInfo = namedtuple("DbInfo", "engine url jdbc_url dialect load_table host db_name")
# end_resources_0
def get_conn_string(
    username: str,
    password: str,
    hostname: str,
    db_name: str,
    port: str,
    scheme: str = "postgresql"
) -> str:
    return f"{scheme}://{quote(username)}:{quote(password)}@{hostname}:{port}/{db_name}"

POSTGRES_CONFIG = {
    "con_string": get_conn_string(
        username= EnvVar("uid"),
        password=EnvVar("pwd"),
        hostname= EnvVar("hostname"),
        port= EnvVar("port"),
        db_name= EnvVar("db"),
        scheme = 'postgresql'
    )
}

api_key = EnvVar("api_key")
base_url = EnvVar("base_url")
core_url = EnvVar("core_url")

# We expect the dbt project to be installed as package data.
# For details, see https://docs.python.org/3/distutils/setupscript.html#installing-package-data.
dbt_project_dir = Path(__file__).joinpath("..", "..", "dbt-project").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")




# def create_postgres_db_url(username, password, hostname, port, db_name, jdbc=True):
#     if jdbc:
#         db_url = (
#             "jdbc:postgresql://{hostname}:{port}/{db_name}?"
#             "user={username}&password={password}".format(
#                 username=username, password=password, hostname=hostname, port=port, db_name=db_name
#             )
#         )
#     else:
#         db_url = "postgresql://{username}:{password}@{hostname}:{port}/{db_name}".format(
#             username=username, password=password, hostname=hostname, port=port, db_name=db_name
#         )
#     return db_url

# def create_postgres_engine(db_url):
#     return sqlalchemy.create_engine(db_url)

# # start_resources_marker_1
# @resource(
#     {
#         "username": Field(StringSource),
#         "password": Field(StringSource),
#         "hostname": Field(StringSource),
#         "port": Field(IntSource, is_required=False, default_value=5432),
#         "db_name": Field(StringSource),
#     }
# )
# def postgres_db_info_resource(init_context):
#     host = init_context.resource_config["hostname"]
#     db_name = init_context.resource_config["db_name"]

#     db_url_jdbc = create_postgres_db_url(
#         username=init_context.resource_config["username"],
#         password=init_context.resource_config["password"],
#         hostname=host,
#         port=init_context.resource_config["port"],
#         db_name=db_name,
#     )

#     db_url = create_postgres_db_url(
#         username=init_context.resource_config["username"],
#         password=init_context.resource_config["password"],
#         hostname=host,
#         port=init_context.resource_config["port"],
#         db_name=db_name,
#         jdbc=False,
#     )

#     def _do_load(data_frame, table_name):
#         data_frame.write.option("driver", "org.postgresql.Driver").mode("overwrite").jdbc(
#             db_url_jdbc, table_name
#         )

#     return DbInfo(
#         url=db_url,
#         jdbc_url=db_url_jdbc,
#         engine=create_postgres_engine(db_url),
#         dialect="postgres",
#         load_table=_do_load,
#         host=host,
#         db_name=db_name,
#     )