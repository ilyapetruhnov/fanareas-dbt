from dagster import asset
from dagster_fanareas.facts.facts import Facts
import random
from dagster_fanareas.facts.queries import top_players_query, top_teams_query, top_team_stats_query

season = random.randint(2008,2023)
top_n = 10
top_n_by_team = 5
metric_list = ['goals','assists','yellow_cards']
teams_metric_list = ['goals','assists','goal_assists','yellow_cards','penalties']

@asset(group_name="facts")
def publish_all_facts():
    facts_obj = Facts(top_players_query, season, top_n, metric_list)
    for metric in facts_obj.metric_list:
        facts_obj.post_facts(metric)
    return True

@asset(group_name="facts")
def publish_one_fact():
    facts_obj = Facts(top_players_query, season, top_n, metric_list)
    metric = random.choice(metric_list)
    facts_obj.post_facts(metric)
    return True

@asset(group_name="facts")
def publish_one_fact_by_team():
    facts_obj = Facts(top_teams_query, season, top_n_by_team, teams_metric_list)
    teams_metric_list.remove('penalties')
    metric = random.choice(teams_metric_list)
    facts_obj.post_facts(metric, by_team=True)
    return True

@asset(group_name="facts")
def publish_player_season_stats_fact():
    facts_obj = Facts(top_team_stats_query, season, top_n_by_team, teams_metric_list)
    metric = random.choice(teams_metric_list)
    facts_obj.post_facts(metric)
    return True