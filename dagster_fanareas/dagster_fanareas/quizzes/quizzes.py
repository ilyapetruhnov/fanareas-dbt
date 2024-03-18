import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session
from dagster_fanareas.quizzes.queries import *

class Quizzes:
    def __init__(self, title: str, description: str, quiz_type: int) -> None:
        self.title = title
        self.description = description
        self.quiz_type = quiz_type
        self.url = "https://fanareas.com/api/quizzes/createQuizz"

    def quiz_template(self, questions):
        json_data = {"title": self.title,
                    "type": self.quiz_type,
                    "description": self.description,
                    "questions": questions}
            
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
    

    def mixed_quiz_questions(self, quizzes:list):
        random.shuffle(quizzes)
        result_list = quizzes[:10]
        return result_list


    def post_quiz(self, questions):
        json_data = self.quiz_template(questions)
        return post_json(json_data, self.url)