from dagster import asset
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.ops.utils import create_db_session
import pandas as pd
import requests
import random

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
    season_list = [i for i in range(2012, 2023)]
    season = random.choice(season_list)
    season_name = f"{season}/{season+1}"
    player_metrics = ['goals','yellow_cards','appearances','assists','goal_assists','substituets']
    player_dim_metrics = ['nationality','position','jersey_number']
    random.shuffle(player_metrics)
    title = f"Guess {team_name} player in {season_name} season"
    description = f"Guess 10 {team_name} players in {season_name} season"
    quiz_type = 0
    is_demo = False
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)

    quiz_oldest_player = quiz_obj.generate_player_age_question(
        query = query_team_player_season_dims.format(team_id, season), 
        team_name = team_name, 
        season_name = season_name,
        oldest = True
        )
    quiz_obj.collect_questions(quiz_oldest_player)

    quiz_youngest_player = quiz_obj.generate_player_age_question(
        query = query_team_player_season_dims.format(team_id, season), 
        team_name = team_name, 
        season_name = season_name,
        oldest = False
        )
    quiz_obj.collect_questions(quiz_youngest_player)

    quiz_sent_off = quiz_obj.generate_player_sent_off_question(
        query = query_team_player_season_dims.format(team_id, season), 
        team_name = team_name, 
        season_name = season_name
        )
    quiz_obj.collect_questions(quiz_sent_off)

    for metric in player_dim_metrics:
        quiz_team_player_dims = quiz_obj.generate_player_metric_question(
            query = query_team_player_season_dims.format(team_id, season),
            metric = metric, 
            season_name=season_name,
        )
        quiz_obj.collect_questions(quiz_team_player_dims)
    
    
    for metric in player_metrics:
        quiz_team_player_stats = quiz_obj.generate_player_stats_question(
            query = query_team_player_season_stats.format(team_id, season), 
            season_name=season_name,
            metric = metric,
        )
        quiz_obj.collect_questions(quiz_team_player_stats)

        quiz_team_player_stats_n = quiz_obj.generate_player_more_than_n_question(
            n = 5,
            query = query_team_player_season_stats.format(team_id, season), 
            season_name=season_name,
            metric_lst = player_metrics,
        )
        quiz_obj.collect_questions(quiz_team_player_stats_n)

    mixed_quiz_questions = quiz_obj.mix_quiz_questions()
    quiz_obj.post_quiz(questions=mixed_quiz_questions,
                       team_name=team_name,
                       season_name=season_name,
                       entityIdTeam=team_id,
                       entityIdSeason=21646,
                       entityTypeTeam=1, 
                       entityTypeSeason=2
                       )

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
    quiz_obj.collect_questions(quiz_player_transferred_from_to)
    quiz_player_2_clubs_played = quiz_obj.generate_quiz_questions(query_player_2_clubs_played, 
                                                                  statement_player_2_clubs_played, 
                                                                 ('team', 
                                                                  'transfer_from_team')
    )
    quiz_obj.collect_questions(quiz_player_2_clubs_played)
    mixed_quiz_questions = quiz_obj.mix_quiz_questions()
    quiz_obj.post_quiz(mixed_quiz_questions)
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
    quiz_obj.post_quiz(mixed_quiz_questions)

    return True