from dagster import asset
import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session


# @asset( group_name="templates", compute_kind="pandas", io_manager_key="db_io_manager")
# def quiz_player_shirt_number(context) -> list:

#     team_id = 19
#     team_df = context.resources.db_io_manager.load_players_unnest_query(context, input_team_id = team_id)
#     result_lst = []
#     for i in range(2):
#         sample_df = team_df.sample(n=4)
#         correct_idx = random.randint(0, 3)
#         correct_row = sample_df.iloc[correct_idx]
#         shirt_number = correct_row['shirt_number']
#         team_name = correct_row['team']
#         question = f"Which player played for {team_name} under {shirt_number} shirt number?"
#         options = list(sample_df['fullname'])
#         correct_response = correct_row['fullname']
#         result = {
#             "question": question,
#             "options": options,
#             "response": correct_response
#             }
#         result_lst.append(result)
#     return result_lst


# @asset( group_name="templates", compute_kind="pandas", io_manager_key="db_io_manager")
# def quiz_player_two_clubs_played(context) -> list:

#     team_df = context.resources.db_io_manager.load_players_two_clubs_query(context)
#     result_lst = []
#     for i in range(4):
#         sample_df = team_df.sample(n=4)
#         correct_idx = random.randint(0, 3)
#         correct_row = sample_df.iloc[correct_idx]
#         clubname1 = correct_row['team_arr'][0]
#         clubname2 = correct_row['team_arr'][1]
#         question = f"Which player played for {clubname1} and {clubname2}?"
#         options = list(sample_df['fullname'])
#         correct_response = correct_row['fullname']
#         result = {
#             "question": question,
#             "options": options,
#             "response": correct_response
#             }
#         result_lst.append(result)

#     return result_lst



@asset( group_name="templates", compute_kind="pandas")
def quiz_player_transferred_from_to() -> dict:
    # Create a connection to the database
    engine = create_db_session()
    # connection = engine.connect()

    # Execute a simple SELECT query
    query =  f"""
                with vw as (SELECT player_id
                    , lastname
                    , fullname
                    , nationality
                    , date_of_birth
                    , t.team                                as team_arr
                    , array_to_string(t.team, ',')          as team
                    , array_to_string(t.team_id, ',')       as team_id
                    , array_to_string(t.jersey_number, ',') as jersey_number
                    , t.season
                    , t.captain
                    , t.yellow_cards
                    , t.red_cards
                    , t.yellow_red_cards
                    , t.minutes_played
                    , t.appearances
                    , t.assists
                    , t.lineups
                    , t.goals
                    , t.home_yellow_cards
                    , t.penalties
                    , t.own_goals
                    , t.goals_conceded
                FROM dim_players
                        CROSS JOIN UNNEST(season_stats) AS t
                WHERE current_season = 2023
                )
    select *
    from vw
    where array_length(team_arr, 1) > 1
    """
    # Read data from the SQL table into a DataFrame
    team_df = pd.read_sql(query, con=engine)

    # team_df = context.resources.db_io_manager.load_players_two_clubs_query()
    q_lst = []
    for i in range(10):
        sample_df = team_df.sample(n=4)
        correct_idx = random.randint(0, 3)
        correct_row = sample_df.iloc[correct_idx]
        clubname1 = correct_row['team_arr'][0]
        clubname2 = correct_row['team_arr'][1]
        season = correct_row['season']
        question = f"Which player had a transfer from {clubname1} to {clubname2} in the {season} season?"
        options = list(sample_df['fullname'])
        correct_response = correct_row['fullname']
        question = {
        "question": question,
        "quizQuestionOptions": options,
        "correctAnswer": correct_response
                    }
        q_lst.append(question)


    json_data = {"title": "Guess the player", 
                 "description": "Answer 10 questions",
                 'questions': q_lst}

    return json_data

@asset(group_name="templates")
def post_player_transferred_from_to_quiz(quiz_player_transferred_from_to: dict) -> bool:
    return post_json(quiz_player_transferred_from_to)





# @asset( group_name="templates", compute_kind="pandas", io_manager_key="db_io_manager")
# def quiz_player_clubname_age(context) -> dict:

#     team_id = 19
#     team_df = context.resources.db_io_manager.load_players_unnest_query(context, input_team_id = team_id)
#     team_df['full_name'] = team_df.apply(lambda x: x['first_name'] + ' ' + x['last_name'], axis=1)
#     sample_df = team_df.sample(n=4)
#     correct_idx = random.randint(0, 3)
#     correct_row = sample_df.iloc[correct_idx]
#     shirt_number = correct_row['shirt_number']
#     team_name = correct_row['team']
#     question = f"Which player played for {team_name} under {shirt_number} shirt number?"
#     options = list(sample_df['full_name'])
#     correct_response = correct_row['full_name']
#     result = {
#         "question": question,
#         "options": options,
#         "response": correct_response
#         }
#     return result