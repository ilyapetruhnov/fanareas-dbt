from dagster import asset
from quizzes import Quizzes
from queries import *
import random

@asset(group_name="quizzes")
def guess_the_player_quiz() -> bool:
    title = "Guess the player"
    description = "Guess 10 football players from the Premier League"
    quiz_type = 0
    quiz_obj = Quizzes(title, description, quiz_type)

    quiz_player_shirt_number = quiz_obj.generate_quiz_questions(query_player_shirt_number, 
                                                                statement_player_shirt_number, 
                                                                'team', 
                                                                'jersey_number')

    quiz_player_2_clubs_played = quiz_obj.generate_quiz_questions(query_player_2_clubs_played, 
                                                                  statement_player_2_clubs_played, 
                                                                  'team', 
                                                                  'transfer_from_team')

    quiz_player_age_nationality = quiz_obj.generate_quiz_questions(query_player_age_nationality, 
                                                                   statement_player_age_nationality,
                                                                   'birth_year', 
                                                                   'nationality')

    quiz_player_age_team = quiz_obj.generate_quiz_questions(query_player_age_team, 
                                                            statement_player_age_team,
                                                            'team', 
                                                            'birth_year')

    quiz_player_height = quiz_obj.generate_simple_questions(query_player_height, 
                                                          statement_player_height,
                                                          dimension = 'height',
                                                          query_param = random.randint(0, 40))

    combined_q_list = quiz_player_height + quiz_player_shirt_number + quiz_player_2_clubs_played + quiz_player_age_nationality + quiz_player_age_team
    mixed_quiz_questions = quiz_obj.mixed_quiz_questions(combined_q_list)

    return quiz_obj.post_quiz(mixed_quiz_questions)


@asset(group_name="quizzes")
def transfers_quiz() -> bool:
    title = "Daily transfers"
    description = "Answer 10 questions about Premier League transfers"
    quiz_type = 1
    quiz_obj = Quizzes(title, description, quiz_type)
    quiz_player_transferred_from_to = quiz_obj.generate_quiz_questions(query_player_transferred_from_to, 
                                                                       statement_player_transferred_from_to, 
                                                                       'transfer_from_team', 
                                                                       'team', 
                                                                       'season')
    quiz_player_2_clubs_played = quiz_obj.generate_quiz_questions(query_player_2_clubs_played, 
                                                                  statement_player_2_clubs_played, 
                                                                  'team', 
                                                                  'transfer_from_team')

    combined_q_list = quiz_player_2_clubs_played + quiz_player_transferred_from_to

    mixed_quiz_questions = quiz_obj.mixed_quiz_questions(combined_q_list)

    return quiz_obj.post_quiz(mixed_quiz_questions)