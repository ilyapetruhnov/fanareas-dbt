from dagster import asset
from facts import Facts
import random
from facts.queries import top_players_query, top_teams_query

query = top_players_query
season = random.randint(2008,2023)
top_n = 10
metric_list = ['goals','assists','yellow_cards','red_cards','minutes_played']

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