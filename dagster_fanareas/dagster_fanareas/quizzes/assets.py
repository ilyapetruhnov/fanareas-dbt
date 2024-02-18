from dagster import asset
import pandas as pd
import random
from dagster_fanareas.ops.utils import api_call, fetch_data, flatten_list, upsert
from dagster_fanareas.constants import base_url





@asset( group_name="transfers", compute_kind="pandas", io_manager_key="db_io_manager")
def quiz_player_shirt_number(context) -> pd.DataFrame:

    # random_int = np.random(1, 3800) # total_player_count
    team_id = 19
    team_df = context.resources.db_io_manager.load_players_unnest_query(context, input_team_id = team_id)
    team_df['full_name'] = team_df.apply(lambda x: x['first_name'] + ' ' + x['last_name'], axis=1)
    sample_df = team_df.sample(n=4)
    correct_idx = random.randint(0, 3)
    correct_row = sample_df.iloc[correct_idx]
    shirt_number = correct_row['shirt_number']
    team_name = correct_row['team']
    question = f"Which player played for {team_name} under {shirt_number} shirt number?"
    options = list(sample_df['full_name'])
    correct_response = correct_row['full_name']
    result = {
        "question": question,
        "options": options,
        "response": correct_response
        }
    return result