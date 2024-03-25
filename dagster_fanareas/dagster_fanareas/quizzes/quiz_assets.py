from dagster import asset
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.ops.utils import create_db_session
import pandas as pd
import requests

def get_team_name_and_id() -> dict:
    engine = create_db_session()
    team_id = requests.get('https://fanareas.com/api/teams/generateId').json()
    team_qr = """select name from teams where id = {}""".format(team_id)
    df = pd.read_sql(team_qr, con=engine)
    team_name = df['name'].iloc[0]
    return {'team_name': team_name, 'team_id': team_id}

@asset(group_name="quizzes")
def guess_team_player_quiz() -> bool:
    generated_team = get_team_name_and_id()
    team_name = generated_team['team_name']
    team_id = generated_team['team_id']
    title = f"Guess {team_name} player"
    description = f"Guess 10 {team_name} players"
    quiz_type = 0
    is_demo = False
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)

    quiz_team_player_age_team = quiz_obj.generate_question(query_team_player_age.format(team_id), 
                                                            statement_player_age_team,
                                                             (team_name, 
                                                            'team', 
                                                             'birth_year')
    )

    combined_q_list = []
    for i in range(3):
        quiz_team_player_shirt_number = quiz_obj.generate_question(query_team_player_shirt_number.format(team_id), 
                                                        statement_team_player_shirt_number, 
                                                        (team_name, 
                                                         'team',
                                                        'jersey_number')
        )
        quiz_team_player_club_transferred_from = quiz_obj.generate_question(query_team_player_club_transferred_from.format(team_id), 
                                                                  statement_team_player_club_transferred_from, 
                                                                 ('fullname', 
                                                                 team_name)
        )

        quiz_team_player_age_nationality = quiz_obj.generate_question(query_team_player_age_nationality.format(team_id), 
                                                                   statement_team_player_age_nationality,
                                                                    (team_name, 
                                                                   'birth_year', 
                                                                    'nationality')
        )
        combined_q_list.append(quiz_team_player_shirt_number)
        combined_q_list.append(quiz_team_player_club_transferred_from)
        combined_q_list.append(quiz_team_player_age_nationality)
        
    combined_q_list.append(quiz_team_player_age_team)

    mixed_quiz_questions = quiz_obj.mixed_quiz_questions(combined_q_list)
    quiz_obj.post_quiz(mixed_quiz_questions)

    return True


@asset(group_name="quizzes")
def guess_the_player_quiz() -> bool:
    title = "Guess the player"
    description = "Guess 10 football players from the Premier League"
    quiz_type = 0
    is_demo = False
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)

    quiz_player_shirt_number = quiz_obj.generate_quiz_questions(query_player_shirt_number, 
                                                                statement_player_shirt_number, 
                                                                ('team', 'jersey_number')
    )

    quiz_player_2_clubs_played = quiz_obj.generate_quiz_questions(query_player_2_clubs_played, 
                                                                  statement_player_2_clubs_played, 
                                                                 ('team', 'transfer_from_team')
    )

    quiz_player_age_nationality = quiz_obj.generate_quiz_questions(query_player_age_nationality, 
                                                                   statement_player_age_nationality,
                                                                   ('birth_year', 'nationality')
    )

    quiz_player_age_team = quiz_obj.generate_quiz_questions(query_player_age_team, 
                                                            statement_player_age_team,
                                                            ('team', 'birth_year')
    )

    # quiz_player_height = quiz_obj.generate_simple_questions(query_player_height, 
    #                                                       statement_player_height,
    #                                                       dimension = 'height',
    #                                                       )

    combined_q_list = quiz_player_shirt_number + quiz_player_2_clubs_played + quiz_player_age_nationality + quiz_player_age_team

    mixed_quiz_questions = quiz_obj.mixed_quiz_questions(combined_q_list)
    quiz_obj.post_quiz(mixed_quiz_questions)

    return True


@asset(group_name="quizzes")
def transfers_quiz() -> bool:
    title = "Daily transfers"
    description = "Answer 10 questions about Premier League transfers"
    quiz_type = 1
    is_demo = False
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_player_transferred_from_to = quiz_obj.generate_quiz_questions(query_player_transferred_from_to, 
                                                                       statement_player_transferred_from_to, 
                                                                       ('transfer_from_team', 
                                                                       'team', 
                                                                       'season')
    )
    quiz_player_2_clubs_played = quiz_obj.generate_quiz_questions(query_player_2_clubs_played, 
                                                                  statement_player_2_clubs_played, 
                                                                 ('team', 
                                                                  'transfer_from_team')
    )

    combined_q_list = quiz_player_2_clubs_played + quiz_player_transferred_from_to

    mixed_quiz_questions = quiz_obj.mixed_quiz_questions(combined_q_list)
    quiz_obj.post_quiz(mixed_quiz_questions)

    return True

@asset(group_name="quizzes")
def demo_quiz() -> bool:
    title = "English Premier League"
    description = "Answer 10 questions about Premier League"
    quiz_type = -1
    is_demo = True
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    combined_q_list = []
    combined_q_list.append(quiz_obj.generate_player_shirt_number_question())
    combined_q_list.append(quiz_obj.generate_player_2_clubs_question())
    combined_q_list.append(quiz_obj.generate_team_stats_question())
    combined_q_list.append(quiz_obj.generate_team_stats_question())
    combined_q_list.append(quiz_obj.generate_venue_question())
    combined_q_list.append(quiz_obj.generate_founded_question())
    combined_q_list.append(quiz_obj.generate_capacity_question())
    combined_q_list.append(quiz_obj.generate_fewest_points_question())
    combined_q_list.append(quiz_obj.generate_most_points_question())
    combined_q_list.append(quiz_obj.generate_relegations_question())

    mixed_quiz_questions = quiz_obj.mixed_quiz_questions(combined_q_list)
    quiz_obj.post_quiz(mixed_quiz_questions)

    return True