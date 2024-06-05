import random
from dagster_fanareas.ops.utils import get_dim_name_and_id
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.transfers_queries import transfers_query
from dagster_fanareas.quizzes.quizzes import Quizzes
from datetime import datetime

class TransferQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
    
    def generate_player_transfer_question(self, clubs_played=False) -> dict:
        df = self.generate_df(transfers_query)
        generated_team = get_dim_name_and_id('teams')
        team_from = generated_team['name']
        df['date'] = df['date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
        df['month'] = df['date'].apply(lambda x: x.strftime('%B'))
        df['year'] = df['date'].apply(lambda x: x.year)
        try:
            correct_df = df[df['from_team'] == team_from].sample(1)
        except Exception as e:
            return None
        correct_response = correct_df['fullname'].iloc[0]
        year = correct_df['year'].iloc[0]
        month = correct_df['month'].iloc[0]
        team_to = correct_df['to_team'].iloc[0]
        if clubs_played:
            options_df = df[(df['from_team']!= team_from) & (df['to_team']!= team_from)].sample(3)
            question_statement = "Who played for {} and {} in his career?".format(team_from, team_to)
        else:
            options_df = df.drop([correct_df.index[0]], axis=0).sample(3)
            question_statement = "Which player left {} and joined {} in {} {}?".format(team_from, team_to, month, year)

        options = [i for i in options_df['fullname']]
        options.append(correct_response)
        if correct_response in self.players:
            return None
        question = self.question_template(question_statement, options, correct_response)
        self.players.append(correct_response)
        return question
    
    def generate_player_left_joined_question(self, joined=False) -> dict:
        generated_team = get_dim_name_and_id('teams')
        team = generated_team['name']
        team_id = generated_team['id']
        df = self.generate_df(transfers_query)
        df['date'] = df['date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
        df['month'] = df['date'].apply(lambda x: x.strftime('%B'))
        df['year'] = df['date'].apply(lambda x: x.year)
        if joined:
            selected_df = df[df['to_team_id'] == team_id]
            if selected_df.empty:
                return None
            correct_df = selected_df.sample(1)
            correct_response = correct_df['fullname'].iloc[0]
            year = correct_df['year'].iloc[0]
            month = correct_df['month'].iloc[0]
            options_df = df[df['year'] != year].sample(3)
            question_statement = "Who joined {} in {} {}?".format(team, month, year)
        else:
            selected_df = df[ df['from_team_id'] == team_id]
            if selected_df.empty:
                return None
            correct_df = selected_df.sample(1)
            correct_response = correct_df['fullname'].iloc[0]
            year = correct_df['year'].iloc[0]
            month = correct_df['month'].iloc[0]
            options_df = df[df['year'] != year].sample(3)
            question_statement = "Who left {} in {} {}?".format(team, month, year)
        
        options = [i for i in options_df['fullname']]
        options.append(correct_response)
        if correct_response in self.players:
            return None
        question = self.question_template(question_statement, options, correct_response)
        self.players.append(correct_response)
        return question
    
    def get_question(self):
        i = random.randint(1,2)
        if i == 1:
            question = self.generate_player_transfer_question(clubs_played=False)

        elif i == 2:
            question = self.generate_player_transfer_question(clubs_played=True)

        # elif i == 3:
        #     question = self.generate_player_left_joined_question(joined=False)

        # elif i == 4:
        #     question = self.generate_player_left_joined_question(joined=True)
            
        return question 
    
    def fill_quiz_with_questions(self):
        while len(self.quiz_collection) < 10:
            question = self.get_question()
            self.collect_questions(question)
            