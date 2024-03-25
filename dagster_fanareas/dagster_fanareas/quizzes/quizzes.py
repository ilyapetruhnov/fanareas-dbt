import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session
from dagster_fanareas.quizzes.queries import *
import requests

class Quizzes:
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        self.title = title
        self.description = description
        self.quiz_type = quiz_type
        self.is_demo = is_demo
        self.url = "https://fanareas.com/api/quizzes/createQuizz"

    def quiz_template(self, questions):
        json_data = {"title": self.title,
                    "type": self.quiz_type,
                    "description": self.description,
                    "questions": questions,
                    "isDemo": self.is_demo}
            
        return json_data
    
    def format_metric(self, metric: str) -> str:
        if metric == 'penalties':
            result = 'penalty goals'
        else:
            result = metric.replace('_',' ')
        return result

    def generate_df(self, query: str) -> pd.DataFrame:
        engine = create_db_session()
        return pd.read_sql(query, con=engine)

    def generate_question(self, query: str, statement: str, team_name: str, cols: tuple) -> list:
        df = self.generate_df(query)
        
        
        sample_df = df.sample(n=4)
        correct_idx = random.randint(0, 3)
        correct_row = sample_df.iloc[correct_idx]
        correct_vals = [correct_row[i] for i in cols]
        question_statement = statement.format(team_name, *correct_vals)
        options = list(sample_df['fullname'])
        correct_response = correct_row['fullname']
        question = {
        "description": question_statement,
        "quizQuestionOptions": options,
        "correctAnswer": correct_response
                    }
        return question

    def generate_quiz_questions(self, query: str, statement: str, cols: tuple) -> list:
        df = self.generate_df(query)
        
        q_lst = []
        for i in range(10):
            dimension = cols[0]
            val_dim = df[df[dimension].map(df[dimension].value_counts()) > 4][dimension].value_counts().index.unique()[i]
            
            sample_df = df[df[dimension]==val_dim].sample(n=4)
            correct_idx = random.randint(0, 3)
            correct_row = sample_df.iloc[correct_idx]
            correct_vals = [correct_row[i] for i in cols]
            question_statement = statement.format(*correct_vals)
            options = list(sample_df['fullname'])
            correct_response = correct_row['fullname']
            question = {
            "description": question_statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
            q_lst.append(question)
        return q_lst
    
    def generate_simple_questions(self, query: str, statement: str,  dimension: str):
        df = self.generate_df(query)
        q_lst = []
        for i in range(10):
            sample_df = df.sample(n=4).sort_values(dimension)
            options = list(sample_df['fullname'])
            correct_response = sample_df.iloc[3]['fullname']
            question = {
            "description": statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
            q_lst.append(question)
        return q_lst
    
    def generate_team_questions(self, query: str, statement: str,  dimension: str):
        df = self.generate_df(query)
        seasons = list(df['season'].unique())[2:]
        metrics = [
            'losses', 'wins', 'draws', 
            'goals', 'goals_conceded', 
            'yellow_cards', 'red_cards',
            'clean_sheets', 'corners'
                    ]
        q_lst = []
        counter = 0
        # check out dagster project linkedin
        while counter < 10:
            dim = random.choice(metrics)
            season = random.choice(seasons)
            formatted_dim = self.format_metric(dim)
            correct_vals = (formatted_dim, season)
            dimension = f'{dim}_rn'
            sample_df = df[(df['season'] == season) & (df[dimension]<=4)]
            correct_row = sample_df[sample_df[dimension]==1]
            if len(correct_row) > 1:
                pass
            else:
                counter+=1
                options = list(sample_df['team'])
                
                correct_response = correct_row['team'].iloc[0]
                
                question_statement = statement.format(*correct_vals)
                question = {
                    "description": question_statement,
                    "quizQuestionOptions": options,
                    "correctAnswer": correct_response
                                }
                q_lst.append(question)
        return q_lst
    
    def generate_player_shirt_number_question(self):
        df = self.generate_df(query_player_shirt_number)
        teamid = requests.get('https://fanareas.com/api/teams/generateId').json()
        sample_df = df[df['team_id']==teamid].sample(4)
        team = sample_df['team'].iloc[0]
        jersey_number = sample_df['jersey_number'].iloc[0]
        correct_response = sample_df['fullname'].iloc[0]
        options = list(sample_df['fullname'])
        random.shuffle(options)
        statement = f"Which player currently plays for {team} under {jersey_number} jersey number?"
        question = {
        "description": statement,
        "quizQuestionOptions": options,
        "correctAnswer": correct_response
                    }
        return question

    def generate_player_2_clubs_question(self):

        df = self.generate_df(query_player_2_clubs_played)
        sample_df = df.drop_duplicates('player_id').sample(n=4)
        club1 = sample_df['transfer_from_team'].iloc[0]
        club2 = sample_df['team'].iloc[0]
        options = list(sample_df['fullname'])
        random.shuffle(options)
        correct_response = sample_df.iloc[0]['fullname']
        statement = f"Which player played for {club1} and {club2} in his career?"
        question = {
        "description": statement,
        "quizQuestionOptions": options,
        "correctAnswer": correct_response
                    }
        return question

    def generate_team_stats_question(self):
        df = self.generate_df(query_team_stats)
        seasons = list(df['season'].unique())
        metrics = [
            'losses', 'wins', 'draws', 
            'goals', 'goals_conceded', 
            'yellow_cards', 'red_cards',
            'clean_sheets', 'corners'
                    ]
        # check out dagster project linkedin
        dim = random.choice(metrics)
        season = random.choice(seasons)
        formatted_dim = self.format_metric(dim)
        correct_vals = (formatted_dim, season)
        dimension = f'{dim}_rn'
        sample_df = df[df['season'] == season].drop_duplicates(subset=['season',dimension], keep='first').sort_values(dimension)[['team','season',dimension]].head(4)

        correct_row = sample_df[sample_df[dimension] == 1]
        statement_team_stats = "Which team had the most {} in the {} season?"

        options = list(sample_df['team'])
        
        correct_response = correct_row['team'].iloc[0]
        
        question_statement = statement_team_stats.format(*correct_vals)
        question = {
            "description": question_statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
        return question

    def generate_venue_question(self):
        df = self.generate_df(query_capacity_venue)
        sample_df = df[~df['team'].isin(['Brentford','Swansea City','Tottenham Hotspur'])].sample(4)[['team','venue']]
        correct_response = sample_df.iloc[0]['venue']
        correct_team = sample_df.iloc[0]['team']
        statement = f"What is the home venue of {correct_team}?"
        options = list(sample_df['venue'])
        random.shuffle(options)
        question = {
            "description": statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
        return question
    
    def generate_founded_question(self):
        df = self.generate_df(query_capacity_venue)
        sample_df = df.sample(4).sort_values('founded_rn')
        correct_response = sample_df.iloc[0]['team']
        statement = f"Which team was founded first?"
        options = list(sample_df['team'])
        random.shuffle(options)
        question = {
            "description": statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
        return question
    
    def generate_capacity_question(self):
        df = self.generate_df(query_capacity_venue)
        df['venue_city'] = df['venue'] + ' ' + '('+df['city']+')'
        sample_df = df.sample(4).sort_values('capacity_rn')
        correct_response = sample_df.iloc[0]['venue_city']
        statement = f"Which stadium has higher capacity?"
        options = list(sample_df['venue_city'])
        random.shuffle(options)
        question = {
            "description": statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
        return question
    
    def generate_fewest_points_question(self):
        df = self.generate_df(query_standings)
        sample_df = df.head(4)
        correct_response = sample_df.iloc[0]['team']
        statement = f"Which team finished the 2007/2008 season with 11 points?"
        options = list(sample_df['team'])
        random.shuffle(options)
        question = {
            "description": statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
        return question
    
    def generate_most_points_question(self):
        df = self.generate_df(query_standings)
        sample_df = df.sort_values('points', ascending=False).head(30)
        correct_response = sample_df.iloc[0]['team']
        statement = f"Which team finished the 2017/2018 season with 100 points (English Premier League record)?"
        options = list(sample_df['team'].unique())[:4]
        random.shuffle(options)
        question = {
            "description": statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
        return question
    
    def generate_relegations_question(self):
        df = self.generate_df(query_relegations)
        sample_df = df.sample(1)
        correct_response = sample_df.iloc[0]['team_promoted']
        season = sample_df.iloc[0]['season']
        options = sample_df.iloc[0]['options']
        random.shuffle(options)
        statement = f"Which team did not relegate in the {season} season?"
        question = {
            "description": statement,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
        return question

    def mixed_quiz_questions(self, quizzes:list):
        random.shuffle(quizzes)
        result_list = quizzes[:10]
        return result_list

    def post_quiz(self, questions):
        json_data = self.quiz_template(questions)
        return post_json(json_data, self.url)