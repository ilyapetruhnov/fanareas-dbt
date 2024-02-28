from dagster import asset
import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session

def guess_player_template(query:str, statement: str, *cols):
    engine = create_db_session()
    df = pd.read_sql(query, con=engine)
    
    q_lst = []
    for i in range(10):
        dimension = cols[0]
        dimension = df[dimension].unique()[i]
        sample_df = df[df[dimension]==dimension].sample(n=4)
        correct_idx = random.randint(0, 3)
        correct_row = sample_df.iloc[correct_idx]
        correct_vals = [correct_row[i] for i in cols]
        question = statement.format(*correct_vals)
        options = list(sample_df['fullname'])
        correct_response = correct_row['fullname']
        question = {
        "description": question,
        "quizQuestionOptions": options,
        "correctAnswer": correct_response
                    }
        q_lst.append(question)
        json_data = {"title": "Guess the player", 
                 "description": "Answer 10 questions",
                 'questions': q_lst}
        
    return json_data


@asset( group_name="templates", compute_kind="pandas")
def quiz_player_shirt_number(context) -> dict:
    query="""
        SELECT
        firstname,
        lastname,
        fullname,
        array_to_string(team, '/') as team,
        array_to_string(jersey_number, '/') as jersey_number,
        t.season
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and array_length(t.team,1) = 1
        and is_active = true
        """
    statement = "Which player currently plays for {} under {} shirt number?"
    json_data = guess_player_template(query, statement, 'team', 'jersey_number')
    return json_data

@asset( group_name="templates", compute_kind="pandas")
def quiz_player_age_nationality(context) -> dict:
    query="""
        SELECT
        firstname,
        lastname,
        fullname,
        nationality,
        date_part('year', cast(date_of_birth as date) ) as birth_year,
        array_to_string(team, '/') as team,
        array_to_string(jersey_number, '/') as jersey_number,
        t.season
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and date_of_birth is not null
        and array_length(t.team,1) = 1
        and is_active = true
        """
    statement = "Which player has a nationality {} and was born in {}?"

    json_data = guess_player_template(query, statement, 'nationality', 'birth_year')
    return json_data

@asset( group_name="templates", compute_kind="pandas")
def quiz_player_age_team(context) -> dict:
    query="""
        SELECT
        firstname,
        lastname,
        fullname,
        nationality,
        date_part('year', cast(date_of_birth as date) ) as birth_year,
        array_to_string(team, '/') as team,
        array_to_string(jersey_number, '/') as jersey_number,
        t.season
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and date_of_birth is not null
        and array_length(t.team,1) = 1
        and is_active = true
        """
    statement = "Which player currently plays for team {} and was born in {}?"
    json_data = guess_player_template(query, statement, 'team', 'birth_year')
    return json_data


@asset( group_name="templates", compute_kind="pandas")
def quiz_player_2_clubs_played(context) -> dict:
    query="""
            WITH vw as (
            SELECT
            player_id,
            firstname,
            lastname,
            lag(array_to_string(team, '/')) over
                (partition by player_id order by t.season) transfer_from_team,
            array_to_string(team, '/') as team,
            array_to_string(jersey_number, '/') as jersey_number,
            team as team_arr,
            t.season as season,
            t.season_name
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            current_season = 2023
            AND array_length(team, 1) = 1
            )
            select * from vw
            where team != transfer_from_team
    """
    statement = "Which player played for {} and {} in his career?"
    
    json_data = guess_player_template(query, statement, 'team', 'transfer_from_team')
    return json_data

@asset( group_name="templates", compute_kind="pandas")
def quiz_player_transferred_from_to(context) -> dict:
    query="""
            WITH vw as (
            SELECT
            player_id,
            firstname,
            lastname,
            fullname,
            lag(array_to_string(team, '/')) over
                (partition by player_id order by t.season) transfer_from_team,
            array_to_string(team, '/') as team,
            array_to_string(jersey_number, '/') as jersey_number,
            team as team_arr,
            t.season as season,
            t.season_name
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            current_season = 2023
            AND array_length(team, 1) = 1
            )
            select * from vw
            where team != transfer_from_team
    """
    statement = "Which player had a transfer from {} to {} in the {} season?"
    
    json_data = guess_player_template(query, statement, 'transfer_from_team', 'team', 'season')
    return json_data


@asset(group_name="templates")
def post_quiz_player_shirt_number(quiz_player_shirt_number: dict) -> bool:
    return post_json(quiz_player_shirt_number)

@asset(group_name="templates")
def post_quiz_player_age_nationality(quiz_player_age_nationality: dict) -> bool:
    return post_json(quiz_player_age_nationality)

@asset(group_name="templates")
def post_quiz_player_age_team(quiz_player_age_team: dict) -> bool:
    return post_json(quiz_player_age_team)

@asset(group_name="templates")
def post_quiz_player_2_clubs_played(quiz_player_2_clubs_played: dict) -> bool:
    return post_json(quiz_player_2_clubs_played)

@asset(group_name="templates")
def post_quiz_player_transferred_from_to(quiz_player_transferred_from_to: dict) -> bool:
    return post_json(quiz_player_transferred_from_to)
