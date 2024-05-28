import pandas as pd
import random
from dagster_fanareas.ops.utils import get_dim_name_and_id
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.quizzes.queries import photo_query

class PhotoQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
    
    def generate_player_by_photo_question(self) -> dict:
        generated_team = get_dim_name_and_id('teams')
        team_id = generated_team['id']
        df = self.generate_df(photo_query.format(team_id))

        options_df = df.sample(4)
        correct_response = options_df['fullname'].iloc[0]
        question_statement = options_df['image_path'].iloc[0]
        options = [i for i in options_df['fullname']]
 
        if correct_response in self.players:
            return None
        question = self.question_template(question_statement, options, correct_response)
        self.players.append(correct_response)
        return question
    
    def fill_quiz_with_questions(self):
        while len(self.quiz_collection) < 5:
            question = self.generate_player_by_photo_question()
            self.collect_questions(question)
            