from dagster import asset
import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session

def fact_template(quiz_type: int, title: str, description: list):
    json_data = {
        "title": title,
        "description": description,
        "type": quiz_type
        }
    return json_data

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
                        order by assists desc nulls last
                        )
                select *
                from vw1
                where assists_rn <= 10
                or goals_rn <= 10
                or red_cards_rn <= 10
                or yellow_cards_rn <= 10
        """
    engine = create_db_session()
    df = pd.read_sql(query, con=engine)
    return df


def top_ten(season: int, metric: str) -> dict:
    df = top_10_stats(season)
    df = df[df[f'{metric}_rn']<=10][['fullname','team','season_name',metric]].sort_values(metric, ascending=False)
    season_name = df['season_name'].iloc[0]
    title = f"Premier League {season_name}: Top ten players with the most {metric}"
    quiz_type = 0
    description = []
    for idx, row in df.iterrows():
        d = {
            "name": row['fullname'],
            "number": int(row[metric])
        }
        description.append(d)

    result = fact_template(quiz_type, title, description[:10])
    return result

def post_facts(metric) -> bool:
    url = "https://fanareas.com/api/facts/createFact"
    season = random.randint(2008,2023)
    json_data = top_ten(season, metric)

    return post_json(json_data, url)

@asset(group_name="facts")
def publish_facts():
    fact_list = ['goals','assists','yellow_cards','red_cards']
    for metric in fact_list:
        post_facts(metric)
    return True


