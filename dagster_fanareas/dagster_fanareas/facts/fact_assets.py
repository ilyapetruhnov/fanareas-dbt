from dagster import asset
from dagster_fanareas.facts.facts import Facts
import random
from dagster_fanareas.facts.queries import top_teams_query, top_season_stats_query

# @asset(group_name="facts")
# def publish_one_fact():
#     facts_obj = Facts(top_players_query, season, top_n, metric_list)
#     metric = random.choice(metric_list)
#     facts_obj.post_facts(metric)
#     return True

@asset(group_name="facts")
def publish_one_fact_by_team():
    season = random.randint(2008,2023)
    facts_obj = Facts(query = top_teams_query, season = season, top_n = 5)
    metrics = facts_obj.metrics()
    metrics.remove('penalties')
    metrics.remove('red_cards')
    metric = random.choice(metrics)
    facts_obj.post_facts(metric, by_team=True)
    return True

@asset(group_name="facts")
def publish_player_season_stats_fact():
    season = random.randint(2008,2023)
    facts_obj = Facts(query = top_season_stats_query, season = season, top_n = 5)
    metrics = facts_obj.metrics()
    metrics.remove('minutes_played')
    metrics.remove('lineups')
    metric = random.choice(metrics)
    facts_obj.post_facts(metric)
    return True