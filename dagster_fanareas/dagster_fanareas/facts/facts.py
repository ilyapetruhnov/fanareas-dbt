import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session
from sqlalchemy import text
import requests

class Facts:
    def __init__(self, query: str, top_n: int) -> None:
        self.query = query
        self.top_n = top_n
        self.url = "https://fanareas.com/api/facts/createFact"

    def fact_template(self, season_name: str, quiz_type: int, title: str, facts: list):
        json_data = {
            "season": season_name,
            "title": title,
            "description": facts,
            "type": quiz_type
            }
        return json_data
    
    def metrics(self):
        metric_list = ['goals',
                       'assists',
                       'goals_assists',
                       'yellow_cards',
                       'red_cards',
                       'penalties',
                       'minutes_played',
                       'lineups',
                       'substitute_appearances']
        return metric_list

    def format_metric(self, metric: str) -> str:
        if metric == 'penalties':
            result = 'penalty goals'
        if metric == 'goals_assists':
            result = 'goals + assists'
        if metric == 'substitute_appearances':
            result = 'appearances coming off the bench'
        if metric == 'goals_all_count':
            result = 'scored goals'
        if metric == 'goals_conceded_all_count':
            result = 'conceded goals'
        if metric == 'scoring_minutes_75_90_count':
            result = 'goals scored after 75 minute'
        if metric == 'cleansheets_count':
            result = 'cleansheets'
        if metric == 'corners_count':
            result = 'corners'
        if metric == 'yellowcards_count':
            result = 'yellow cards '
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
    
    def get_random_season(self):
        seasons = [2020,2021,2022,2023]
        return random.choice(seasons)
    
    def get_season_str(self, season: int):
        return f"{season}/{season+1}"
    
    @staticmethod 
    def combine(x,y):
        return f"goals: {int(x)}, assists: {int(y)}"

    def generate_df(self, query_str: str, season) -> pd.DataFrame:
        engine = create_db_session()
        query = query_str.format(season)
        return pd.read_sql(text(query), con=engine)

    def top_n_facts_assembler(self, query, metric: str, season: str, by_team=False) -> dict:
        df = self.generate_df(query, season)
        metric_filter = (df[f'{metric}_rn'] <= self.top_n)
        metric_formatted = self.format_metric(metric)
        col_list = ['fullname','team','season_name', metric]
        season_name = df['season_name'].iloc[0]
        quiz_type = 0
        # df['goals_assists'] = df.apply(lambda x: self.combine(x['goals'],x['assists']),axis=1)
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
    

    def team_facts(self) -> dict:
        season = self.get_random_season()
        season_name = self.get_season_str(season)
        df = self.generate_df(self.query, season_name)
    
        metrics = [
                'goals_all_count',
                'goals_conceded_all_count',
                'scoring_minutes_75_90_count',
                'cleansheets_count',
                'corners_count',
                'yellowcards_count',
                'num_of_goals_over_3_5_team_count'
        ]
        metric = random.choice(metrics)
        metric_formatted = self.format_metric(metric)
        col_list = ['team','season', metric]
        quiz_type = 0
        df = df[col_list].sort_values(metric, ascending=False)
        if metric == 'num_of_goals_over_3_5_team_count':
            title = f"Premier League {season_name}: Top 5 teams with the most 4+ goals scored games"
        else:
            title = f"Premier League {season_name}: Top 5 teams with the most {metric_formatted}"
            
        top_facts = []
        for idx, row in df.iterrows():
            d = {
                "name": row['team'],
                "number": int(row[metric])
            }
            top_facts.append(d)
        facts = top_facts[:self.top_n]
        return self.fact_template(season_name, quiz_type, title, facts)


    def post_facts(self, metric: str, season: int, by_team=False) -> bool:
        if by_team:
            json_data = self.top_n_facts_assembler(self.query, metric, season, by_team=True)
        else:
            json_data = self.top_n_facts_assembler(self.query, metric, season)
        return post_json(json_data, self.url)
    
    def post_team_facts(self) -> bool:
        json_data = self.team_facts()
        return post_json(json_data, self.url)
