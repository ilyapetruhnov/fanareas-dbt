import random
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.tm_queries import player_query, top_value_players_query
from dagster_fanareas.quizzes.quizzes import Quizzes

class PlayerQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
        self.nationality_mapping = {
            'Netherlands': 'Dutch',
            'Brazil': 'Brazilian',
            'France': 'French',
            'Germany': 'German',
            'Spain': 'Spanish'
                                    }
    
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
        drop_teams = ['France Olympic Team',
             'France U23',
              'France U21',
              'England U21',
              '',
              'Spain U21',
              'Spain Olympic Team',
              'Brazil U23'
             ]

        df = df[~df['international_team'].isin(drop_teams)]
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
        shirt_number = random.choice(['9','10'])
        if shirt_number == '9':
            ndf = df[df['player_shirt_number']=='9']
            options_df = ndf.sample(4)
            correct_response = options_df['player_name'].iloc[0]
            team_name = options_df['team'].iloc[0]
            options = [i for i in options_df['player_name']]
            question_statement = f"Who is the {team_name} number nine?"
        else:
            ndf = df[df['player_shirt_number']=='10']
            options_df = ndf.sample(4)
            correct_response = options_df['player_name'].iloc[0]
            team_name = options_df['team'].iloc[0]
            options = [i for i in options_df['player_name']]
            question_statement = f"Who is the {team_name} number ten?"

        question = self.question_template(question_statement, options, correct_response)
        return question


    def player_position_question(self) -> dict:
        df = self.generate_df(top_value_players_query)
        ndf = df.groupby('position_group').apply(lambda x: x.sample(1)).reset_index(drop=True)
        options_df = ndf.sample(4)

        position_group = options_df['position_group'].iloc[0]
        player_main_position = options_df['player_main_position'].iloc[0]
        team_name = options_df['team'].iloc[0]
        options = [i for i in options_df['player_name']]
        correct_response = options_df['player_name'].iloc[0]
        # age = options_df['age'].iloc[0]


        if position_group == 'midfielder':
            question_statement1 = f"Which {position_group}er, known for his playmaking abilities and vision, plays for {team_name}?"
            question_statement2 = f"Which palyer plays as {player_main_position}er at {team_name}?"
            question_statement3 = f"Who occupies the {player_main_position} position at {team_name}?"
            question_statement4 = f"Which midfielder is on the {team_name} roster?"
        # elif age < 21:
        #     question_statement = f"Which young {position_group} plays for {team_name}?"
        else:
            question_statement1 = f"Which {player_main_position} plays for {team_name}?"
            question_statement2 = f"Which {player_main_position} is part of {team_name} squad?"
            question_statement3 = f"Who is the {team_name} {player_main_position}?"
            question_statement4 = f"Which {player_main_position} represents {team_name}?"
        

        question_statement = random.choice([question_statement1,question_statement2, question_statement3, question_statement4])
        question = self.question_template(question_statement, options, correct_response)
        return question
    
        # elif position_group == 'forward':
        #     question_statement1 = f"Who is the Argentine forward leading the line for Inter Milan?"
        

    def transfer_and_club_question(self) -> dict:
        return None
    
    def played_for_multiple_clubs(self) -> dict:
        return None
    

    def played_for_multiple_clubs(self) -> dict:
        return None
    
    def player_position_club_performance(self) -> dict:

        f"Who is the Dutch {position_group} and {team_name} player, known for his defensive prowess and has scored over {goals} goals in {league_name}?"
        f"Which Senegalese {position_group} currently plays for {team_name}, has scored over {goals} goals and provided over {assists} assists in {league_name}?"
    




        
    