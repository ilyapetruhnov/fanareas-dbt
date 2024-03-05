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
    
    def generate_query(self, query_str: str, query_param) -> str:
        if query_param:
            return query_str.format(query_param)
        return query_str

    def generate_df(self, query: str) -> pd.DataFrame:
        engine = create_db_session()
        return pd.read_sql(query, con=engine)

    def generate_quiz_questions(self, query_str: str, statement: str,  query_param: None, *cols: str) -> list:
        query = self.generate_query(query_str, query_param)
        df = self.generate_df(query)
        
        q_lst = []
        for i in range(10):
            dimension = cols[0]
            val_dim = df[df[dimension].map(df[dimension].value_counts()) > 4][dimension].value_counts().index.unique()[i]
            
            sample_df = df[df[dimension]==val_dim].sample(n=4)
            correct_idx = random.randint(0, 3)
            correct_row = sample_df.iloc[correct_idx]
            correct_vals = [correct_row[i] for i in cols]
            question = statement.format(*correct_vals)
            options = list(sample_df['fullname'])
            correct_response = correct_row['fullname']
            question = {
            "description": question,
            "quizQuestionOptions": options,
            "correctAnswer": correct_response
                        }
            q_lst.append(question)
        return q_lst
    
    def generate_simple_questions(self, query_str: str, statement: str,  dimension: str, query_param: None):
        query = self.generate_query(query_str, query_param)
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
    

    def mixed_quiz_questions(*quizzes:list):
        combined_q_list = [x + y for x, y in quizzes] # DEBUG
        random.shuffle(combined_q_list)
        result_list = combined_q_list[:10]
        return result_list


    def post_quiz(self, questions):
        json_data = self.quiz_template(questions)
        return post_json(json_data, self.url)