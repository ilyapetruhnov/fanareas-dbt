from dagster import asset
import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session

def quiz_template(quiz_type: int, title:str, description: str, result_list: list):
    json_data = {"title": title,
                "type": quiz_type,
                "description": description,
                "questions": result_list}
        
    return json_data


# def generate_quiz_questions(query: str, statement: str, *cols) -> list:
#     engine = create_db_session()
#     df = pd.read_sql(query, con=engine)
#     if cols:
#         q_lst = []
#         for i in range(10):
#             dimension = cols[0]
#             val_dim = df[df[dimension].map(df[dimension].value_counts()) > 4][dimension].value_counts().index.unique()[i]
#             # val_dim = df[dimension].unique()[i]
#             sample_df = df[df[dimension]==val_dim].sample(n=4)
#             correct_idx = random.randint(0, 3)
#             correct_row = sample_df.iloc[correct_idx]
#             correct_vals = [correct_row[i] for i in cols]
#             question = statement.format(*correct_vals)
#             options = list(sample_df['fullname'])
#             correct_response = correct_row['fullname']
#             question = {
#             "description": question,
#             "quizQuestionOptions": options,
#             "correctAnswer": correct_response
#                         }
#             q_lst.append(question)
#     else:
#         q_lst = []
#         for i in range(10):

#             sample_df = df.sample(n=4).sort_values('height')
#             # height = sample_df.iloc[3]['height']
#             # player_name = sample_df.iloc[3]['player_name']
#             # correct_vals = [correct_row[i] for i in cols]
#             question = statement.format(*correct_vals)
#             options = list(sample_df['fullname'])
#             correct_response = sample_df.iloc[3]['player_name']
#             question = {
#             "description": question,
#             "quizQuestionOptions": options,
#             "correctAnswer": correct_response
#                         }
#             q_lst.append(question)

#     return q_lst


def generate_quiz_questions(query: str, statement: str, *cols) -> list:
    engine = create_db_session()
    df = pd.read_sql(query, con=engine)
    
    q_lst = []
    for i in range(10):
        dimension = cols[0]
        val_dim = df[df[dimension].map(df[dimension].value_counts()) > 4][dimension].value_counts().index.unique()[i]
        # val_dim = df[dimension].unique()[i]
        sample_df = df[df[dimension]==val_dim].sample(n=4)
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
    return q_lst


@asset( group_name="templates", compute_kind="pandas")
def quiz_player_shirt_number(context) -> list:

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
    statement = "Which player currently plays for {} under {} jersey number?"
    q_list = generate_quiz_questions(query, statement, 'team', 'jersey_number')
    return q_list

@asset( group_name="templates", compute_kind="pandas")
def quiz_player_age_nationality(context) -> list:
    query="""
        with vw as (
        SELECT
        firstname,
        lastname,
        fullname,
        nationality,
        date_of_birth,
        array_to_string(team, '/') as team,
        t.season,
        lead(nationality, 1) over (order by nationality, date_of_birth) as next_nationality,
        lead(date_of_birth, 1) over (order by nationality, date_of_birth) as next_date_of_birth
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and date_of_birth is not null
        and array_length(t.team,1) = 1
        and is_active = true),
        window_vw as (
        select
        fullname,
        nationality,
        next_nationality,
        cast(date_part('year', cast(date_of_birth as date)) as int) as birth_year,
        cast(date_part('year', cast(next_date_of_birth as date)) as int) as next_birth_year
        from vw)
        select * 
        from window_vw
        where
        next_nationality != nationality
        or next_birth_year != birth_year
        """
    statement = "Which Premier League player was born in {} in {}?"
    q_list = generate_quiz_questions(query, statement, 'birth_year', 'nationality')

    return q_list

@asset( group_name="templates", compute_kind="pandas")
def quiz_player_age_team(context) -> list:
    query="""
            with vw as (
            SELECT
            firstname,
            lastname,
            fullname,
            nationality,
            date_of_birth,
            array_to_string(team, '/') as team,
            t.season,
            lead(array_to_string(team, '/'), 1) over (order by array_to_string(team, '/'), date_of_birth) as next_team,
            lead(date_of_birth, 1) over (order by array_to_string(team, '/'), date_of_birth) as next_date_of_birth
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            t.season = 2023
            and date_of_birth is not null
            and array_length(t.team,1) = 1
            and is_active = true),
            window_vw as (
            select
            fullname,
            team,
            next_team,
            cast(date_part('year', cast(date_of_birth as date)) as int) as birth_year,
            cast(date_part('year', cast(next_date_of_birth as date)) as int) as next_birth_year
            from vw)
            select * 
            from window_vw
            where
            next_team != team
            or next_birth_year != birth_year
            """
    statement = "Which player currently plays for {} and was born in {}?"
    q_list = generate_quiz_questions(query, statement, 'team', 'birth_year')

    return q_list


@asset( group_name="templates", compute_kind="pandas")
def quiz_player_2_clubs_played(context) -> list:

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
            ), window_vw as (
            SELECT
            *
            ,lead(transfer_from_team, 1) over (order by team, transfer_from_team, season_name) as next_transfer_from_team
            ,lead(team, 1) over (order by team, transfer_from_team, season_name) as next_team
            ,lead(season_name, 1) over (order by team, transfer_from_team, season_name) as next_season_name
            from vw)
            SELECT
                player_id,
                fullname,
                transfer_from_team,
                team,
                season,
                season_name
                from window_vw
                        where
                        team != transfer_from_team
                        AND
                (next_transfer_from_team != transfer_from_team
                or next_team != team
                or next_season_name != season_name
                )
        """
    statement = "Which player played for {} and {} in his career?"
    q_list = generate_quiz_questions(query, statement, 'team', 'transfer_from_team')
    return q_list

@asset( group_name="templates", compute_kind="pandas")
def quiz_player_transferred_from_to(context) -> list:
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
            ), window_vw as (
            SELECT
            *
            ,lead(transfer_from_team, 1) over (order by team, transfer_from_team, season_name) as next_transfer_from_team
            ,lead(team, 1) over (order by team, transfer_from_team, season_name) as next_team
            ,lead(season_name, 1) over (order by team, transfer_from_team, season_name) as next_season_name
            from vw)
            SELECT
                player_id,
                fullname,
                transfer_from_team,
                team,
                season,
                season_name
                from window_vw
                        where
                        team != transfer_from_team
                        AND
                (next_transfer_from_team != transfer_from_team
                or next_team != team
                or next_season_name != season_name
                )
        """
    statement = "Which player had a transfer from {} to {} in the {} season?"
    q_list = generate_quiz_questions(query, statement, 'transfer_from_team', 'team', 'season')
    return q_list

@asset( group_name="templates", compute_kind="pandas")
def quiz_player_height(context) -> list:
    statement = "Guess the tallest player from the following players"
    i = random.randint(0, 40)
    query=f"""
            with vw as (
                    SELECT
                    height,
                    array_agg(fullname) as fullname
                    FROM
                    dim_players
                    where height is not null
                    and height != 0
                    and is_active = true
            group by height
            )
            select
            height,
            fullname[{i}] AS fullname
            from vw
        """
    engine = create_db_session()
    df = pd.read_sql(query, con=engine)
    q_lst = []
    for i in range(10):

        sample_df = df.sample(n=4).sort_values('height')
        options = list(sample_df['fullname'])
        correct_response = sample_df.iloc[3]['fullname']
        question = {
        "description": statement,
        "quizQuestionOptions": options,
        "correctAnswer": correct_response
                    }
        q_lst.append(question)
    
    return q_lst


@asset(group_name="templates")
def post_guess_the_player_quiz(quiz_player_height:list, quiz_player_2_clubs_played: list, quiz_player_age_nationality:list, quiz_player_age_team: list, quiz_player_shirt_number: list) -> bool:
    title = "Guess the player"
    description = "Guess 10 football players from the Premier League"
    combined_q_list = quiz_player_height + quiz_player_2_clubs_played+ quiz_player_age_nationality + quiz_player_age_team + quiz_player_shirt_number
    random.shuffle(combined_q_list)
    result_list = combined_q_list[:10]
    quiz_type=0
    json_data = quiz_template(quiz_type, title, description, result_list)

    return post_json(json_data)


@asset(group_name="templates")
def post_transfers_quiz(quiz_player_transferred_from_to: list, quiz_player_2_clubs_played: list) -> bool:
    title = "Daily transfers"
    description = "Answer 10 question about Premier League transfers"
    combined_q_list = quiz_player_transferred_from_to + quiz_player_2_clubs_played
    random.shuffle(combined_q_list)
    result_list = combined_q_list[:10]
    quiz_type=1
    json_data = quiz_template(quiz_type, title, description, result_list)

    return post_json(json_data)
