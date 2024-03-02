from dagster import asset
import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session
from sqlalchemy import create_engine, text

def fact_template(season_name: str, quiz_type: int, title: str, description: list):
    json_data = {
        "season": season_name,
        "title": title,
        "description": description,
        "type": quiz_type
        }
    return json_data


def top_5_stats_by_team(season: int) -> pd.DataFrame:
    query = f"""
                with vw as (
                SELECT
                        player_id,
                        fullname,
                        array_to_string(team, '/') as team,
                        t.season_name,
                        t.assists as assists,
                        t.goals as goals,
                        t.red_cards as red_cards,
                        t.yellow_cards as yellow_cards,
                        t.minutes_played as minutes_played
                        FROM
                        dim_players
                        CROSS JOIN UNNEST (season_stats) AS t
                        WHERE
                        current_season = 2023
                        AND
                        t.season = {season}
                ),
                vw1 as (
                select *
                            , row_number() over (partition by team ORDER BY assists desc nulls last)      as assists_rn
                            , row_number() over (partition by team ORDER BY goals desc nulls last)        as goals_rn
                            , row_number() over (partition by team ORDER BY red_cards desc nulls last)    as red_cards_rn
                            , row_number() over (partition by team ORDER BY yellow_cards desc nulls last) as yellow_cards_rn
                            , row_number() over (partition by team ORDER BY minutes_played desc nulls last) as minutes_played_rn
                        from vw
                        )
                select *
                from vw1
                where
                team not LIKE '%/%'
                AND (goals_rn <= 5
                or minutes_played_rn <= 5
                or assists_rn <= 5
                or red_cards_rn <= 5
                or yellow_cards_rn <= 5)
    """
    engine = create_db_session()
    df = pd.read_sql(text(query), con=engine)
    return df


def top_10_stats(season: int) -> pd.DataFrame:
    query=f"""
                with vw as (
                SELECT
                        player_id,
                        fullname,
                        array_to_string(team, '/') as team,
                        t.season_name,
                        t.assists as assists,
                        t.goals as goals,
                        t.red_cards as red_cards,
                        t.yellow_cards as yellow_cards
                        FROM
                        dim_players
                        CROSS JOIN UNNEST (season_stats) AS t
                        WHERE
                        current_season = 2023
                        AND
                        t.season = {season}
                ),
                vw1 as (
                select *
                            , row_number() over (ORDER BY assists desc nulls last)      as assists_rn
                            , row_number() over (ORDER BY goals desc nulls last)        as goals_rn
                            , row_number() over (ORDER BY red_cards desc nulls last)    as red_cards_rn
                            , row_number() over (ORDER BY yellow_cards desc nulls last) as yellow_cards_rn
                        from vw
                        )
                select *
                from vw1
                where assists_rn <= 10
                or goals_rn <= 10
                or red_cards_rn <= 10
                or yellow_cards_rn <= 10
        """
    engine = create_db_session()
    df = pd.read_sql(text(query), con=engine)
    return df


def top_five_by_team(season: int, selected_metric: str) -> dict:
    df = top_5_stats_by_team(season)
    teams = list(df['team'].unique())
    selected_team = random.choice(teams)
    df = df[(df[f'{selected_metric}_rn']<=5) & (df['team']==selected_team)][['fullname','team','season_name', selected_metric]].sort_values(selected_metric, ascending=False)
    season_name = df['season_name'].iloc[0]
    metric_formatted = selected_metric.replace('_',' ')
    title = f"Premier League {season_name}: Top 5 {selected_team} players with the most {metric_formatted}"
    quiz_type = 0
    description = []
    for idx, row in df.iterrows():
        d = {
            "name": row['fullname'],
            "number": int(row[selected_metric])
        }
        description.append(d)

    result = fact_template(season_name, quiz_type, title, description[:5])
    return result

def top_ten(season: int, metric: str) -> dict:
    df = top_10_stats(season)
    df = df[df[f'{metric}_rn']<=10][['fullname','team','season_name', metric]].sort_values(metric, ascending=False)
    season_name = df['season_name'].iloc[0]
    metric_formatted = metric.replace('_',' ')
    title = f"Premier League {season_name}: Top ten players with the most {metric_formatted}"
    quiz_type = 0
    description = []
    for idx, row in df.iterrows():
        d = {
            "name": row['fullname'],
            "number": int(row[metric])
        }
        description.append(d)

    result = fact_template(season_name, quiz_type, title, description[:10])
    return result

def post_facts(metric) -> bool:
    url = "https://fanareas.com/api/facts/createFact"
    season = random.randint(2008,2023)
    json_data = top_ten(season, metric)

    return post_json(json_data, url)

def post_one_fact(selected_metric) -> bool:
    url = "https://fanareas.com/api/facts/createFact"
    season = random.randint(2008,2023)
    json_data = top_five_by_team(season, selected_metric)

    return post_json(json_data, url)

@asset(group_name="facts")
def publish_facts():
    fact_list = ['goals','assists','yellow_cards','red_cards']
    for metric in fact_list:
        post_facts(metric)
    return True

@asset(group_name="facts")
def publish_one_fact():
    metric_list = ['goals','assists','yellow_cards','red_cards','minutes_played']
    selected_metric = random.choice(metric_list)
    post_one_fact(selected_metric)
    return True


