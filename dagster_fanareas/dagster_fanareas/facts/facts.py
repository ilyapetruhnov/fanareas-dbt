import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session
from sqlalchemy import text
import requests

class Facts:
    def __init__(self, query_str: str, season: int, top_n: int, metric_list: list) -> None:
        self.query_str = query_str
        self.season = season
        self.top_n = top_n
        self.metric_list = metric_list
        self.url = "https://fanareas.com/api/facts/createFact"

    def fact_template(self, season_name: str, quiz_type: int, title: str, facts: list):
        json_data = {
            "season": season_name,
            "title": title,
            "description": facts,
            "type": quiz_type
            }
        return json_data
    
    def format_metric(self, metric: str) -> str:
        if metric == 'penalties':
            result = 'penalty goals'
        else:
            result = metric.replace('_',' ')
        return result
    
    def get_team_name_and_id(self) -> dict:
        engine = create_db_session()
        team_id = requests.get('https://fanareas.com/api/teams/generateId').json()
        team_qr = """select name from teams where id = {}""".format(team_id)
        df = pd.read_sql(team_qr, con=engine)
        team_name = df['name'].iloc[0]
        return {'team_name': team_name, 'team_id': team_id}

    def generate_query(self, query_str) -> str:
        return query_str.format(self.season)

    def generate_df(self) -> pd.DataFrame:
        engine = create_db_session()
        query = self.generate_query(self.query_str)
        return pd.read_sql(text(query), con=engine)

    def top_n_facts_assembler(self, metric: str, by_team=False) -> dict:
        df = self.generate_df()
        metric_filter = (df[f'{metric}_rn'] <= self.top_n)
        metric_formatted = self.format_metric(metric)
        col_list = ['fullname','team','season_name', metric]
        season_name = df['season_name'].iloc[0]
        quiz_type = 0
        if by_team:
            team_obj = self.get_team_name_and_id() 
            selected_team = team_obj['team_name']
            team_filter = (df['team']==selected_team)
            df = df[ metric_filter & team_filter][col_list].sort_values(metric, ascending=False)
            df = df[df[metric]>0]
            title = f"Premier League {season_name}: Top {self.top_n} {selected_team} players with the most {metric_formatted}"
            
        else:
            df = df[metric_filter][col_list].sort_values(metric, ascending=False)
            df = df[df[metric]>0]
            title = f"Premier League {season_name}: Top {self.top_n} players with the most {metric_formatted}"
            

        top_facts = []
        for idx, row in df.iterrows():
            d = {
                "name": row['fullname'],
                "number": int(row[metric])
            }
            top_facts.append(d)
        facts = top_facts[:self.top_n]
        return self.fact_template(season_name, quiz_type, title, facts)

    def post_facts(self, metric: str, by_team=False) -> bool:
        if by_team:
            json_data = self.top_n_facts_assembler(metric, by_team=True)
        else:
            json_data = self.top_n_facts_assembler(metric)
        return post_json(json_data, self.url)



