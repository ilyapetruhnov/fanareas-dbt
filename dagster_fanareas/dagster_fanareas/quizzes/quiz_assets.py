from dagster import asset
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.quizzes.transferQuizzes import TransferQuizzes
from dagster_fanareas.quizzes.photoQuizzes import PhotoQuizzes
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.ops.utils import get_dim_name_and_id
from dagster_fanareas.quizzes.quiz_collection import validate_team_season, post_guess_team_player_quiz

@asset(group_name="quizzes")
def guess_team_player_quiz() -> bool:
    #team generation
    generated_team = get_dim_name_and_id('teams')
    team_name = generated_team['name']
    team_id = generated_team['id']
    #season generation
    generated_season = get_dim_name_and_id('seasons')
    season_name = generated_season['name']
    season_id = generated_season['id']
    season = int(season_name[:4])
    if validate_team_season(team_id, season_id):
        post_guess_team_player_quiz(team_id, team_name, season, season_id, season_name)
        return True
    else:
        guess_team_player_quiz()
    return True


@asset(group_name="quizzes")
def transfers_quiz() -> bool:
    title = "Daily transfers"
    description = "Answer 5 questions about Premier League transfers"
    quiz_type = 1
    is_demo = False
    quiz_obj = TransferQuizzes(title, description, quiz_type, is_demo)
    quiz_obj.fill_quiz_with_questions()
    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def photo_quiz() -> bool:
    title = "Daily guess the player by photo"
    description = "Guess 5 Premier League players by photo"
    quiz_type = 2
    is_demo = False
    quiz_obj = PhotoQuizzes(title, description, quiz_type, is_demo)
    quiz_obj.fill_quiz_with_questions()
    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


@asset(group_name="quizzes")
def small_demo_quiz() -> bool:
    title = "English Premier League"
    description = "Answer 5 questions about Premier League"
    quiz_type = -1
    is_demo = True
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)

    quiz_obj.collect_questions(quiz_obj.generate_venue_question())
    quiz_obj.collect_questions(quiz_obj.generate_player_played_for_team_question())

    quiz_obj.collect_questions(quiz_obj.generate_capacity_question())
    quiz_obj.collect_questions(quiz_obj.generate_fewest_points_question())

    quiz_obj.collect_questions(quiz_obj.generate_player_shirt_number_question())

    mixed_quiz_questions = quiz_obj.mix_quiz_questions()
    quiz_obj.post_quiz(questions = mixed_quiz_questions)

    return True


@asset(group_name="quizzes")
def demo_quiz() -> bool:
    title = "English Premier League"
    description = "Answer 10 questions about Premier League"
    quiz_type = -1
    is_demo = True
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(quiz_obj.generate_player_shirt_number_question())
    quiz_obj.collect_questions(quiz_obj.generate_player_2_clubs_question())
    quiz_obj.collect_questions(quiz_obj.generate_team_stats_question())
    quiz_obj.collect_questions(quiz_obj.generate_team_stats_question())
    quiz_obj.collect_questions(quiz_obj.generate_venue_question())
    quiz_obj.collect_questions(quiz_obj.generate_founded_question())
    quiz_obj.collect_questions(quiz_obj.generate_capacity_question())
    quiz_obj.collect_questions(quiz_obj.generate_fewest_points_question())
    quiz_obj.collect_questions(quiz_obj.generate_most_points_question())
    quiz_obj.collect_questions(quiz_obj.generate_relegations_question())

    mixed_quiz_questions = quiz_obj.mix_quiz_questions()
    quiz_obj.post_quiz(questions = mixed_quiz_questions)

    return True