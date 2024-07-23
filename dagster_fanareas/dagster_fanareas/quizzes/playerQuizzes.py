import random
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.tm_queries import player_query, top_value_players_query
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
        
    def player_national_team_and_club(self) -> dict:
        df = self.generate_df(top_value_players_query)
        ndf = df.groupby('international_team').apply(lambda x: x.sample(1)).reset_index(drop=True)
        options_df = ndf.sample(4)
        correct_response = options_df['player_name'].iloc[0]
        position_group = options_df['position_group'].iloc[0]
        international_team = options_df['international_team'].iloc[0]
        team_name = options_df['team'].iloc[0]
        options = [i for i in options_df['player_name']]

        question_statement1 = f"Which player represents {international_team} national team and currently plays for {team_name}?"
        question_statement2 = f"A key player for the national team of {international_team}, he also plays for {team_name}. Who is this player?"
        question_statement3 = f"Representing {international_team}, this {position_group} plays for {team_name}. Who is he?"
        question_statement4 = f"A {position_group} for {international_team}, who is now playing at {team_name}. Who is he?"
        question_statement5 = f"Which {international_team} {position_group} plays for {team_name}?"

        question_statement = random.choice([question_statement1,question_statement2, question_statement3, question_statement4, question_statement5])

        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def player_shirt_number(self) -> dict:
        df = self.generate_df(top_value_players_query)
        ndf = df.groupby('international_team').apply(lambda x: x.sample(1)).reset_index(drop=True)
        options_df = ndf.sample(4)
        correct_response = options_df['player_name'].iloc[0]
        position_group = options_df['position_group'].iloc[0]
        international_team = options_df['international_team'].iloc[0]
        team_name = options_df['team'].iloc[0]
        options = [i for i in options_df['player_name']]

        question_statement1 = f"Who is the {team_name} number nine?"
        question_statement2 = f"Who is the {team_name} number ten?"

        question_statement = random.choice([question_statement1,question_statement2])

        question = self.question_template(question_statement, options, correct_response)
        return question


    def player_position_question(self) -> dict:

        position_group = options_df['position_group'].iloc[0]
        f"Which young {position} player plays for {team_name}?"


        if position_group.split(' ')[1] == 'Midfield':
            question_statement1 = f"Which {position_group}er, known for his playmaking abilities and vision, plays for {team_name}?"
            question_statement2 = f"Which palyer plays as {position_group}er at {team_name}?"
            question_statement3 = f"Who occupies the {position_group} position at {team_name}?"
            question_statement4 = f"Which midfielder is on the {team_name} roster?"
        else:
            question_statement1 = f"Which {position_group} plays for {team_name}?"
            question_statement2 = f"Which {position_group} is part of {team_name} squad?"
            question_statement3 = f"Who is the {team_name} {position_group}?"
            question_statement4 = f"Which {position_group} represents {team_name}?"



        
    