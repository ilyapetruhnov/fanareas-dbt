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


ordinal_mapping = {
    1: "1st",
    2: "2nd",
    3: "3rd",
    4: "4th",
    5: "5th",
    6: "6th",
    7: "7th",
    8: "8th",
    9: "9th",
    10: "10th",
    11: "11th",
    12: "12th",
    13: "13th",
    14: "14th",
    15: "15th",
    16: "16th",
    17: "17th",
    18: "18th",
    19: "19th",
    20: "20th"
}

times_mapping = {
    1: "once",
    2: "twice",
    3: "three times",
    4: "four times",
    5: "five times",
    6: "six times",
    7: "seven times"
}

league_mapping = {
    'ES1': 'LaLiga',
    'GB1': 'Premier League',
    'L1': 'Bundesliga',
    'IT1': 'Serie A',
    'FR1': 'Ligue 1'
}

team_mapping = {
'11'  :'Arsenal FC',
'985':'Manchester United',
'631':'Chelsea FC',
'148':'Tottenham Hotspur',
'31':'Liverpool FC',
'131':'FC Barcelona',
'418':'Real Madrid',
'506':'Juventus FC',
'12': 'AS Roma',
'6195':'SSC Napoli',
'46' :'Inter Milan',
'27' :'Bayern Munich',
'16' :'Borussia Dortmund',
'5' :'AC Milan'
}


nationality_mapping = {
    "Afghanistan": "Afghan",
    "Albania": "Albanian",
    "Algeria": "Algerian",
    "Andorra": "Andorran",
    "Angola": "Angolan",
    "Antigua and Barbuda": "Antiguan or Barbudan",
    "Argentina": "Argentine",
    "Armenia": "Armenian",
    "Australia": "Australian",
    "Austria": "Austrian",
    "Azerbaijan": "Azerbaijani",
    "Bahamas": "Bahamian",
    "Bahrain": "Bahraini",
    "Bangladesh": "Bangladeshi",
    "Barbados": "Barbadian",
    "Belarus": "Belarusian",
    "Belgium": "Belgian",
    "Belize": "Belizean",
    "Benin": "Beninese",
    "Bhutan": "Bhutanese",
    "Bolivia": "Bolivian",
    "Bosnia-Herzegovina": "Bosnia-Herzegovinian",
    "Botswana": "Botswanan",
    "Brazil": "Brazilian",
    "Brunei": "Bruneian",
    "Bulgaria": "Bulgarian",
    "Burkina Faso": "Burkinabe",
    "Burundi": "Burundian",
    "Cabo Verde": "Cape Verdean",
    "Cambodia": "Cambodian",
    "Cameroon": "Cameroonian",
    "Canada": "Canadian",
    "Central African Republic": "Central African",
    "Chad": "Chadian",
    "Chile": "Chilean",
    "China": "Chinese",
    "Colombia": "Colombian",
    "Comoros": "Comoran",
    "Ivory Coast": "Ivorian",
    "Cote d'Ivoire": "Ivorian",
    "Democratic Republic of the Congo": "Congolese",
    "Costa Rica": "Costa Rican",
    "Croatia": "Croatian",
    "Cuba": "Cuban",
    "Cyprus": "Cypriot",
    "Czech Republic": "Czech",
    "Denmark": "Danish",
    "Djibouti": "Djiboutian",
    "Dominica": "Dominican",
    "Dominican Republic": "Dominican",
    "Ecuador": "Ecuadorian",
    "Egypt": "Egyptian",
    "El Salvador": "Salvadoran",
    "England": "English",
    "Equatorial Guinea": "Equatoguinean",
    "Eritrea": "Eritrean",
    "Estonia": "Estonian",
    "Eswatini": "Swazi",
    "Ethiopia": "Ethiopian",
    "Fiji": "Fijian",
    "Finland": "Finnish",
    "France": "French",
    "Gabon": "Gabonese",
    "Gambia": "Gambian",
    "Georgia": "Georgian",
    "Germany": "German",
    "Ghana": "Ghanaian",
    "Greece": "Greek",
    "Grenada": "Grenadian",
    "Guatemala": "Guatemalan",
    "Guinea": "Guinean",
    "Guinea-Bissau": "Bissau-Guinean",
    "Guyana": "Guyanese",
    "Haiti": "Haitian",
    "Honduras": "Honduran",
    "Hungary": "Hungarian",
    "Iceland": "Icelandic",
    "India": "Indian",
    "Indonesia": "Indonesian",
    "Iran": "Iranian",
    "Iraq": "Iraqi",
    "Republic of Ireland": "Irish",
    "Ireland": "Irish",
    "Israel": "Israeli",
    "Italy": "Italian",
    "Jamaica": "Jamaican",
    "Japan": "Japanese",
    "Jordan": "Jordanian",
    "Kazakhstan": "Kazakhstani",
    "Kenya": "Kenyan",
    "Kiribati": "I-Kiribati",
    "Korea, North": "North Korean",
    "Korea, South": "South Korean",
    "Kosovo": "Kosovar",
    "Kuwait": "Kuwaiti",
    "Kyrgyzstan": "Kyrgyzstani",
    "Laos": "Lao",
    "Latvia": "Latvian",
    "Lebanon": "Lebanese",
    "Lesotho": "Basotho",
    "Liberia": "Liberian",
    "Libya": "Libyan",
    "Liechtenstein": "Liechtensteiner",
    "Lithuania": "Lithuanian",
    "Luxembourg": "Luxembourgish",
    "Madagascar": "Malagasy",
    "Malawi": "Malawian",
    "Malaysia": "Malaysian",
    "Maldives": "Maldivian",
    "Mali": "Malian",
    "Malta": "Maltese",
    "Marshall Islands": "Marshallese",
    "Mauritania": "Mauritanian",
    "Mauritius": "Mauritian",
    "Mexico": "Mexican",
    "Micronesia": "Micronesian",
    "Moldova": "Moldovan",
    "Monaco": "Monegasque",
    "Mongolia": "Mongolian",
    "Montenegro": "Montenegrin",
    "Morocco": "Moroccan",
    "Mozambique": "Mozambican",
    "Myanmar": "Burmese",
    "Namibia": "Namibian",
    "Nauru": "Nauruan",
    "Nepal": "Nepali",
    "Netherlands": "Dutch",
    "New Zealand": "New Zealander",
    "Nicaragua": "Nicaraguan",
    "Niger": "Nigerien",
    "Nigeria": "Nigerian",
    "North Macedonia": "Macedonian",
    "Norway": "Norwegian",
    "Oman": "Omani",
    "Pakistan": "Pakistani",
    "Palau": "Palauan",
    "Palestine": "Palestinian",
    "Panama": "Panamanian",
    "Papua New Guinea": "Papua New Guinean",
    "Paraguay": "Paraguayan",
    "Peru": "Peruvian",
    "Philippines": "Filipino",
    "Poland": "Polish",
    "Portugal": "Portuguese",
    "Qatar": "Qatari",
    "Romania": "Romanian",
    "Russia": "Russian",
    "Rwanda": "Rwandan",
    "Saint Kitts and Nevis": "Kittitian or Nevisian",
    "Saint Lucia": "Saint Lucian",
    "Saint Vincent and the Grenadines": "Saint Vincentian",
    "Samoa": "Samoan",
    "San Marino": "Sammarinese",
    "Sao Tome and Principe": "Sao Tomean",
    "Saudi Arabia": "Saudi",
    "Scotland": "Scottish",
    "Senegal": "Senegalese",
    "Serbia": "Serbian",
    "Seychelles": "Seychellois",
    "Sierra Leone": "Sierra Leonean",
    "Singapore": "Singaporean",
    "Slovakia": "Slovak",
    "Slovenia": "Slovenian",
    "Solomon Islands": "Solomon Islander",
    "Somalia": "Somali",
    "South Africa": "South African",
    "South Korea": "South Korean",
    "South Sudan": "South Sudanese",
    "Spain": "Spanish",
    "Sri Lanka": "Sri Lankan",
    "Sudan": "Sudanese",
    "Suriname": "Surinamese",
    "Sweden": "Swedish",
    "Switzerland": "Swiss",
    "Syria": "Syrian",
    "Taiwan": "Taiwanese",
    "Tajikistan": "Tajikistani",
    "Tanzania": "Tanzanian",
    "Thailand": "Thai",
    "Timor-Leste": "Timorese",
    "Togo": "Togolese",
    "Tonga": "Tongan",
    "Trinidad and Tobago": "Trinidadian or Tobagonian",
    "Tunisia": "Tunisian",
    "Türkiye": "Turkish",
    "Turkey": "Turkish",
    "Turkmenistan": "Turkmen",
    "Tuvalu": "Tuvaluan",
    "Uganda": "Ugandan",
    "Ukraine": "Ukrainian",
    "United Arab Emirates": "Emirati",
    "United Kingdom": "British",
    "United States": "American",
    "Uruguay": "Uruguayan",
    "Uzbekistan": "Uzbek",
    "Vanuatu": "Ni-Vanuatu",
    "Vatican City": "Vatican",
    "Venezuela": "Venezuelan",
    "Vietnam": "Vietnamese",
    "Wales": "Welsh",
    "Yemen": "Yemeni",
    "Zambia": "Zambian",
    "Zimbabwe": "Zimbabwean"
}


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