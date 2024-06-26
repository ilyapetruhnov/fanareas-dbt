from dagster import asset
from dagster_fanareas.facts.facts import Facts
import random
from dagster_fanareas.facts.queries import top_teams_query, top_season_stats_query, teams_query

@asset(group_name="facts")
def publish_one_fact_by_team():
    facts_obj = Facts(query = top_teams_query, top_n = 5)
    metrics = facts_obj.metrics()
    metrics.remove('penalties')
    metrics.remove('red_cards')
    metric = random.choice(metrics)
    season = random.randint(2008,2023)
    result = facts_obj.post_facts(metric, season, by_team=True)
    if result == False:
        publish_one_fact_by_team()
    return True

@asset(group_name="facts")
def publish_player_season_stats_fact():
    facts_obj = Facts(query = top_season_stats_query, top_n = 5)
    metrics = facts_obj.metrics()
    metrics.remove('minutes_played')
    metrics.remove('lineups')
    metric = random.choice(metrics)
    season = random.randint(2008,2023)
    result = facts_obj.post_facts(metric, season, by_team=False)
    if result == False:
        publish_player_season_stats_fact()
    return True

@asset(group_name="facts")
def publish_team_fact():
    facts_obj = Facts(query = teams_query, top_n = 5)
    result = facts_obj.post_team_facts()
    if result == False:
        publish_team_fact()
    return True