import random
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.tm_queries import player_query
from dagster_fanareas.quizzes.quizzes import Quizzes

class PlayerQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
    
    def player_photo_question(self) -> dict:
        df = self.generate_df(player_query)
        df = df.head(30)
        try:
            options_df = df.sample(4)
            correct_response = options_df['player_name'].iloc[0]
            question_statement = options_df['img'].iloc[0]
            question_statement = "What is the name of this player?" # to be adjusted
            options = [i for i in options_df['player_name']]
            question = self.question_template(question_statement, options, correct_response)
            return question
        except Exception:
            return None
    