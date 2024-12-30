from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.quizzes import Quizzes
import random

class LineupQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.quiz_collection = []

    def create_lineup_question(self, options: list, correct_response: str, question_statement: str):
        img_name = correct_response.replace(" ", "_").lower()
        image_url = f'/lineups/{img_name}.jpg'
        question = self.question_template(
            question_statement = question_statement,
            correct_response = correct_response,
            options = options,
            image_url = image_url,
            image_size = 4
            )
        self.quiz_collection.append(question)
        return True
    
    def format_team_name(self, team_name: str):
        return " ".join(word.capitalize() for word in team_name.split('_'))

    def generate_lineup_question(self, question_statement: str , correct_response: str, team_options: list):
        team_options.remove(correct_response)
        q_options = random.sample(team_options, 3)
        q_options.append(correct_response)

        options = [self.format_team_name(i) for i in q_options]
        self.create_lineup_question(
                options = options,
                correct_response = correct_response,
                question_statement = question_statement
                                )
        return True

    def collect_lineups_questions(self):
        self.clear_collection()
        question_statement = 'Guess the team in 2024/25 season'
        teams = [
            'lazio',
            'napoli',
            'Paris_Saint_Germain',
            'bayern',
            'villareal',
            'juventus',
            'arsenal',
            'Atletico_madrid',
            'manchester_united',
            'barcelona',
            'tottenham',
            'inter',
            'manchester_city',
            'milan',
            'real_madrid',
            'eintracht_frankfurt',
            'liverpool',
            'benfica',
            'borussia_dortmund',
            'chelsea',
            'RB_Leipzig',
            'atalanta',
            'bayer_leverkusen',
            'real_betis',
            'Athletic_bilbao'
            ]
        other_options = [
            'Leicester_City',
            'Crystal_Palace',
            'West_Ham_United',
            'Roma',
            'Aston_Villa',
            'Real_Sociedad',
            'Valencia',
            'Girona',
            'Torino',
            'Fiorentina',
            'Stuttgart',
            'Werder_Bremen',
            'Wolfsburg',
            'Monaco',
            'Olympique_Lyon'
            ]
        q_options = teams + other_options
        random.shuffle(teams)
        for team in teams:
            self.generate_lineup_question(question_statement, team, q_options)
        return self.quiz_collection
    