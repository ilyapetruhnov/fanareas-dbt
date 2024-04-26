from dagster import asset
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.ops.utils import create_db_session, get_dim_name_and_id
import pandas as pd
import random


def validate_team_season(team_id, season_id) -> bool:
    engine = create_db_session()
    query = """select season_id from standings where participant_id = {}""".format(team_id)
    standings_df = pd.read_sql(query, con=engine)
    if season_id in standings_df['season_id'].unique():
        return True
    return False

def post_guess_team_player_quiz(team_id, team_name, season, season_id, season_name) -> bool:
    player_metrics = ['assists','substitute_appearances','goal_assists','yellow_cards','appearances','goals']
    player_dim_metrics = ['nationality','position','jersey_number']
    random.shuffle(player_metrics)
    random.shuffle(player_dim_metrics)
    title = f"Guess {team_name} player in {season_name} Premier League season"
    description = f"Guess 10 {team_name} players in {season_name} Premier League season"
    quiz_type = 0
    is_demo = False
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)

    quiz_player_joined = quiz_obj.generate_player_joined_question(
        query = query_player_joined_club.format(team_id), 
        team_name = team_name, 
        season_name = season_name
        )
    quiz_obj.collect_questions(quiz_player_joined)

    quiz_player_left = quiz_obj.generate_player_departure_question(
        query = query_player_left_club.format(team_id), 
        team_name = team_name, 
        season_name = season_name
        )
    quiz_obj.collect_questions(quiz_player_left)

    quiz_oldest_player = quiz_obj.generate_player_age_question(
        query = query_team_player_season_dims.format(team_id, season), 
        team_name = team_name, 
        season_name = season_name,
        youngest=False
        )
    quiz_obj.collect_questions(quiz_oldest_player)

    quiz_youngest_player = quiz_obj.generate_player_age_question(
        query = query_team_player_season_dims.format(team_id, season), 
        team_name = team_name, 
        season_name = season_name,
        youngest=True
        )
    quiz_obj.collect_questions(quiz_youngest_player)

    quiz_sent_off = quiz_obj.generate_player_sent_off_question(
        query = query_team_player_season_stats.format(team_id, season), 
        season_name = season_name
        )
    quiz_obj.collect_questions(quiz_sent_off)

    quiz_own_goal = quiz_obj.generate_player_own_goal_question(
        query = query_team_player_season_stats.format(team_id, season), 
        season_name = season_name
        )
    quiz_obj.collect_questions(quiz_own_goal)

    quiz_team_player_stats_red_cards = quiz_obj.generate_player_2_metrics_question(
        query = query_team_player_season_stats.format(team_id, season), 
        season_name = season_name,
        metric = 'red_cards'
        )
    quiz_obj.collect_questions(quiz_team_player_stats_red_cards)

    quiz_team_player_stats_goal_assists = quiz_obj.generate_player_2_metrics_question(
        query = query_team_player_season_stats.format(team_id, season), 
        season_name = season_name,
        metric = 'goal_assists'
        )
    quiz_obj.collect_questions(quiz_team_player_stats_goal_assists)

    quiz_player_position_played = quiz_obj.generate_player_position_played_question(
        query = query_team_player_position_season_stats.format(team_id, season), 
        season_name = season_name
        )
    quiz_obj.collect_questions(quiz_player_position_played)


    for metric in player_dim_metrics:
        quiz_team_player_dims = quiz_obj.generate_player_metric_question(
            query = query_team_player_season_dims.format(team_id, season),
            metric = metric, 
            season_name = season_name
        )
        quiz_obj.collect_questions(quiz_team_player_dims)
    
    
    for metric in player_metrics:
        quiz_team_player_stats = quiz_obj.generate_player_stats_question(
            query = query_team_player_season_stats.format(team_id, season), 
            season_name=season_name,
            metric = metric
        )
        quiz_obj.collect_questions(quiz_team_player_stats)

        # quiz_team_player_position_stats = quiz_obj.generate_player_position_stats_question(
        #     query = query_team_player_position_season_stats.format(team_id, season), 
        #     season_name=season_name,
        #     metric = metric
        # )
        # quiz_obj.collect_questions(quiz_team_player_position_stats)

        quiz_team_player_stats_n = quiz_obj.generate_player_more_than_n_question(
            query = query_team_player_season_stats.format(team_id, season), 
            season_name=season_name,
            metric = metric
        )
        quiz_obj.collect_questions(quiz_team_player_stats_n)

    mixed_quiz_questions = quiz_obj.mix_quiz_questions()
    quiz_obj.post_quiz(
        questions = mixed_quiz_questions,
        team_name = team_name,
        season_name = season_name,
        entityIdTeam = team_id,
        entityIdSeason = season_id,
        entityTypeTeam = 1,
        entityTypeSeason = 2
                       )

    return True