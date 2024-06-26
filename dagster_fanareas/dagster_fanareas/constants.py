import os
from pathlib import Path

from dagster_dbt import DbtCliResource
# from collections.abc import namedtuple
# from urllib.parse import quote
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
    return f"{scheme}://{username}:{password}@{hostname}:{port}/{db_name}"

POSTGRES_CONFIG = {
    "con_string": get_conn_string(
        username= os.getenv("POSTGRES_USER"),
        password= os.getenv("POSTGRES_PWD"),
        hostname= os.getenv("POSTGRES_HOST"),
        port= "25060",
        db_name= os.getenv("POSTGRES_DBNAME"),
        scheme = 'postgresql'
    )
}

NEW_POSTGRES_CONFIG = {
    "con_string": get_conn_string(
        username= os.getenv("POSTGRES_USER"),
        password= os.getenv("POSTGRES_PWD"),
        hostname= os.getenv("POSTGRES_HOST"),
        port= "25060",
        db_name= os.getenv("TM_POSTGRES_DBNAME"),
        scheme = 'postgresql'
    )
}

tm_url = 'https://transfermarkt-db.p.rapidapi.com/v1/'
tm_host = 'transfermarkt-db.p.rapidapi.com'
tm_api_key = os.getenv("TM_API_KEY")
api_key = os.getenv("API_KEY")
base_url = "https://api.sportmonks.com/v3/football"
core_url = "https://api.sportmonks.com/v3/core"

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