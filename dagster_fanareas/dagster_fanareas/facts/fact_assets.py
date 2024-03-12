from dagster import asset
from dagster_fanareas.facts.facts import Facts
import random
from dagster_fanareas.facts.queries import top_players_query, top_teams_query

query = top_players_query
query_teams = top_teams_query
season = random.randint(2008,2023)
top_n = 10
metric_list = ['goals','assists','yellow_cards','penalties']
teams_metric_list = ['goals','assists','yellow_cards','penalties','minutes_played','red_cards']

@asset(group_name="facts")
def publish_all_facts():
    facts_obj = Facts(query, season, top_n, metric_list)
    for metric in facts_obj.metric_list:
        facts_obj.post_facts(metric)
    return True

@asset(group_name="facts")
def publish_one_fact():
    facts_obj = Facts(query, season, top_n, metric_list)
    metric = random.choice(metric_list)
    facts_obj.post_facts(metric)
    return True

@asset(group_name="facts")
def publish_one_fact_by_team():
    facts_obj = Facts(query_teams, season, top_n, teams_metric_list)
    metric = random.choice(metric_list)
    facts_obj.post_facts(metric, by_team=True)
    return True