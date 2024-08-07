from dagster import asset, Config, MaterializeResult
import pandas as pd
from dagster_fanareas.ops.tm_utils import tm_fetch_data, rename_camel_col, tm_fetch_squads, tm_fetch_player_performance, tm_fetch_match, tm_fetch_match_stats, tm_fetch_player_profile, tm_fetch_team_profile, tm_fetch_team_info, tm_fetch_team_transfers, tm_fetch_titles, tm_fetch_countries, tm_fetch_competitions, tm_fetch_stuff, tm_fetch_transfer_records, tm_fetch_national_champions, tm_api_call,tm_fetch_referees, tm_fetch_rankings, tm_fetch_competition_info, tm_fetch_competition_champions, tm_fetch_staff_achievements, chunk_list
from dagster_fanareas.constants import tm_url


class LeagueConfig(Config):
    league_id: str

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def season(context) -> pd.DataFrame:
    url = f"{tm_url}competitions/seasons"

    params = {"locale":"US","competition_id":"ES1"}

    df = tm_fetch_data(url ,params)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def standing(context, config: LeagueConfig) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='season')
    seasons = existing_df['id'].unique()
    frames = []
    competiton_id = config.league_id
    for i in seasons:
        params = {"locale":"US",
                    "season_id": i,
                    "standing_type":"general",
                    "competition_id":competiton_id}

        url = f"{tm_url}competitions/standings"
        df = tm_fetch_data(url,params,key='table')
        df['season_id'] = i
        df['season_id'] = df['season_id'].astype(int)
        df['league_id'] = competiton_id
        df.rename(columns={"id": 'team_id'},inplace=True)
        df.rename(columns={"group": 'group_id'},inplace=True)
        for col in df.columns:
            new_col_name = rename_camel_col(col)
            df.rename(columns={col: new_col_name},inplace=True)
        df['team_id'] = df['team_id'].astype(int)
        df['id'] = df.apply(lambda df: eval(f"{df['season_id']}{df['team_id']}"),axis=1)
        frames.append(df)
    return pd.concat(frames)


@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def squad(context, config: LeagueConfig) -> pd.DataFrame:
    standing_df = context.resources.new_io_manager.load_table(table_name='standing')
    league_id = config.league_id
    standing_df = standing_df[standing_df['league_id'] == league_id]
    team_seasons = list(zip(standing_df['team_id'], standing_df['season_id']))
    frames = []
    for i in team_seasons:
        team_id = i[0]
        season_id = i[1]
        try:
            df = tm_fetch_squads(season_id=season_id, team_id=team_id)
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
                df['id'] = df.apply(lambda df: eval(f"{df['player_id']}{df['team_id']}{df['season_id']}"),axis=1)
                df['league_id'] = league_id
            frames.append(df)
        except Exception as e:
            context.log.info(f"Error with season {season_id} and team {team_id}")
    result = pd.concat(frames)
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def player_performace(context, config: LeagueConfig) -> pd.DataFrame:
    squad_df = context.resources.new_io_manager.load_table(table_name='squad')
    league_id = config.league_id
    squad_df = squad_df[(squad_df['league_id'] == league_id) & (squad_df['season_id'] != 2024) ]
    player_seasons = list(zip(squad_df['player_id'], squad_df['season_id']))
    frames = []
    for i in player_seasons:
        player_id = i[0]
        season_id = i[1]
        try:
            df = tm_fetch_player_performance(season_id=season_id, player_id=player_id, league_id=league_id)
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
            frames.append(df)
        except Exception as e:
            context.log.info(f"Error with season {season_id} and player {player_id}")
    result = pd.concat(frames)
    result['league_id'] = league_id
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def matches(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='player_performace')
    all_matches = existing_df['id'].unique()
    matches_dataset = context.resources.new_io_manager.load_table(table_name='matches')
    exisitng_matches = matches_dataset['id'].unique()
    matches = [item for item in all_matches if item not in exisitng_matches]
    # matches = existing_df['id'].unique()
    frames = []
    for match_id in matches:
        try:
            df = tm_fetch_match(match_id)
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
            frames.append(df)
        except Exception as e:
            context.log.info(f"Error with match {match_id}")
    result = pd.concat(frames)
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def match_stats(context) -> pd.DataFrame:
    matches_df = context.resources.new_io_manager.load_table(table_name='matches')
    existing_match_stats_df = context.resources.new_io_manager.load_table(table_name='match_stats')
    all_matches = matches_df['id'].unique()
    existing_match_stats = existing_match_stats_df['match_id'].unique()
    matches = [item for item in all_matches if item not in existing_match_stats]
    context.log.info(len(matches))
    frames = []
    for match_id in matches:
        try:
            df = tm_fetch_match_stats(match_id)
            df.rename(columns={'clubId': 'team_id'},inplace=True)
            frames.append(df)
        except Exception as e:
            context.log.info(f"Error with match {match_id}")
    result = pd.concat(frames)
    cols = ['id','match_id','team_id', 'ballpossession', 'offsides', 'fouls', 'freekicks',
       'cornerkicks', 'goalkeepersaves', 'shotsoffgoal', 'shotsongoal',
       'shotstotal']
    result = result[cols]

    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def player(context, config: LeagueConfig) -> pd.DataFrame:
    squad_df = context.resources.new_io_manager.load_table(table_name='squad')
    player_dataset = context.resources.new_io_manager.load_table(table_name='player')
    league_id = config.league_id
    squad_df = squad_df[squad_df['league_id'] == league_id]
    players_from_squad = squad_df['player_id'].unique()
    players_ingested = player_dataset['id'].unique()
    players = [item for item in players_from_squad if item not in players_ingested]
    context.log.info(len(players))
    frames = []
    for i in players:
        try:
            df = tm_fetch_player_profile(i)
            df.rename(columns={'playerID': 'id','clubID': 'team_id','club': 'team'},inplace=True)
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
            frames.append(df)
        except Exception as e:
            context.log.info(f"Error with player {i}")
    result = pd.concat(frames)
    cols = [
        'id',
        'player_image',
        'player_name',
        'player_full_name',
        'birthplace',
        'date_of_birth',
        'date_of_death',
        'player_shirt_number',
        'birthplace_country',
        'age',
        'height',
        'foot',
        'international_team',
        'country',
        'second_country',
        'league',
        'team',
        'team_id',
        'contract_expiry_date',
        'agent',
        'agent_id',
        'position_group',
        'player_main_position',
        'player_second_position',
        'player_third_position',
        'market_value',
        'market_value_currency',
        'market_value_numeral',
        'market_value_last_change']
    result = result[cols]
    return result

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def team(context, config: LeagueConfig) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='standing')
    league_id = config.league_id
    existing_df = existing_df[existing_df['league_id'] == league_id]
    teams = existing_df['team_id'].unique()
    frames = []
    for i in teams:
        try:
            df_profile = tm_fetch_team_profile(i)
            df_info = tm_fetch_team_info(i)
            df =  pd.concat([df_profile, df_info],axis=1)
            for col in df.columns:
                new_col_name = rename_camel_col(col)
                df.rename(columns={col: new_col_name},inplace=True)
            frames.append(df)
        except Exception as e:
            context.log.info(f"Error with team {i}")
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def transfer(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='team')
    teams = existing_df['id'].unique()
    frames = []
    for i in teams:
        try:
            df = tm_fetch_team_transfers(i)
            if df is not None:
                for col in df.columns:
                    new_col_name = rename_camel_col(col)
                    df.rename(columns={col: new_col_name},inplace=True)
                frames.append(df)
        except Exception as e:
            context.log.info(f"Error with team {i}")
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def transfer_records():
    url = f"{tm_url}markets/transfers-records"
    page_num = 0
    frames = []
    while True:
        params = {
            "locale":"US",
            "page_number": page_num,
            "top_transfers_first": "true"
                  }
        page_num +=1
        response= tm_api_call(url, params)
        if response is None:
            break
        data = response.json()['data']
        frames.append(pd.DataFrame(data))
    if len(frames)>0:
        df = pd.concat(frames)
    else:
        return None
    df['transferFee_value'] = df['transferFee'].apply(lambda x: x['value'])
    df['transferFee_currency'] = df['transferFee'].apply(lambda x: x['currency'])
    df['transferMarketValue_value'] = df['transferMarketValue'].apply(lambda x: x['value'])
    df['transferMarketValue_currency'] = df['transferMarketValue'].apply(lambda x: x['currency'])
    cols = ['id', 'playerID', 'fromClubID', 'toClubID', 'transferredAt', 'isLoan',
        'wasLoan', 'season', 'fromCompetitionID', 'toCompetitionID','transferFee_value',
        'transferFee_currency', 'transferMarketValue_value',
        'transferMarketValue_currency']
    df = df[cols]
    df.rename(columns={'fromClubID': 'from_team_id',
                    'toClubID': 'to_club_id',
                    'playerID': 'player_id',
                    'fromCompetitionID':'from_competition_id',
                    'toCompetitionID':'to_competition_id'
                    }, inplace=True)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def titles(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='team')
    teams = existing_df['id'].unique()
    frames = []
    for i in teams:
        context.log.info(i)
        try:
            df = tm_fetch_titles(i)
            if df is not None:
                for col in df.columns:
                    new_col_name = rename_camel_col(col)
                    df.rename(columns={col: new_col_name},inplace=True)
                frames.append(df)
        except Exception as e:
            context.log.info(f"Error with team {i}")
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def stuff(context) -> pd.DataFrame:
    frames = []
    existing_df = context.resources.new_io_manager.load_table(table_name='team')
    coaches = existing_df['coach_id'].unique()
    for stuff_id in coaches:
        try:
            data = tm_fetch_stuff(stuff_id)
            if data is not None:
                result_df = pd.DataFrame.from_dict(data, orient='index').T
                cols = ['id', 'countryID', 'personID', 'personImage','playerID', 'personnelID',
            'personName', 'firstName', 'lastName', 'alias', 'dateOfBirth',
            'deathDay', 'age', 'birthplace',  'countryImage',
            'countryName', 'averageTermAsCoach']
                result_df = result_df[cols]
                result_df.rename(columns={'personID': 'person_id',
                                        'countryID': 'country_id',
                                        'playerID': 'player_id',
                                        'personnelID': 'personnel_id'},inplace=True)

                frames.append(result_df)
        except Exception as e:
            context.log.info(f"Error with stuff_id {stuff_id}")
    return pd.concat(frames)


@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def country() -> pd.DataFrame:
    frames = []
    data = tm_fetch_countries()
    for i in data:
        result_df = pd.DataFrame.from_dict(i, orient='index').T
        frames.append(result_df)
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def competition_info(context) -> pd.DataFrame:
    competitions_df = context.resources.new_io_manager.load_table(table_name='competitions')
    competitions = competitions_df['id'].unique()
    frames = []
    for i in competitions:
        try:
            data = tm_fetch_competition_info(i)
            result_df = pd.DataFrame.from_dict(data, orient='index').T
            frames.append(result_df)
        except Exception as e:
            context.log.info(f"Error with competition_id {i}")
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def competition_champions(context) -> pd.DataFrame:
    competitions_df = context.resources.new_io_manager.load_table(table_name='competitions')
    competitions = competitions_df['id'].unique()
    frames = []
    for league_id in competitions:
        try:
            data = tm_fetch_competition_champions(league_id)
            df = pd.DataFrame(data['champions'])
            df['league_id'] = league_id
            frames.append(df)
        except Exception as e:
            context.log.info(f"Error with competition_id {league_id}")
    return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def competitions(context) -> pd.DataFrame:
    existing_df = context.resources.new_io_manager.load_table(table_name='country')
    countries = existing_df['id'].unique()

    result = []
    for country_id in countries:
        try:
            frames = []
            data = tm_fetch_competitions(country_id)
            for i in data['children']:
                result_df = pd.DataFrame.from_dict(i, orient='index').T
                frames.append(result_df)
            df = pd.concat(frames)
            df['country'] = data['competitionGroupName']
            df.rename(columns={'competitionGroupCompetitionID': 'id'},inplace=True)
            result.append(df)
        except TypeError:
            pass
    ndf = pd.concat(result)
    for col in ndf.columns:
        new_col_name = rename_camel_col(col)
        ndf.rename(columns={col: new_col_name},inplace=True)
    cols = ['competition_group_name', 
            'id',
            'league_level',
            'country']
    return ndf[cols]

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def euro_champions() -> pd.DataFrame:
    url = f"{tm_url}rankings/european-champions"
    data = tm_fetch_national_champions(url)
    df = pd.DataFrame(data['teams'])
    df.rename(columns={'seasonID': 'season_id'},inplace=True)
    df.rename(columns={'successID': 'success_id'},inplace=True)
    df.rename(columns={'coachID': 'coach_id'},inplace=True)
    for col in df.columns:
        new_col_name = rename_camel_col(col)
        df.rename(columns={col: new_col_name},inplace=True)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def world_champions() -> pd.DataFrame:
    url = f"{tm_url}rankings/world-cup-champions"
    data = tm_fetch_national_champions(url)
    df = pd.DataFrame(data['teams'])
    df.rename(columns={'seasonID': 'season_id'},inplace=True)
    df.rename(columns={'successID': 'success_id'},inplace=True)
    df.rename(columns={'coachID': 'coach_id'},inplace=True)
    for col in df.columns:
        new_col_name = rename_camel_col(col)
        df.rename(columns={col: new_col_name},inplace=True)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def uefa_ranking() -> pd.DataFrame:
    url = f"{tm_url}rankings/uefa"
    data = tm_fetch_rankings(url)
    df = pd.DataFrame(data['teams'])
    for col in df.columns:
        new_col_name = rename_camel_col(col)
        df.rename(columns={col: new_col_name},inplace=True)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def fifa_ranking() -> pd.DataFrame:
    url = f"{tm_url}rankings/fifa"
    data = tm_fetch_rankings(url)
    df = pd.DataFrame(data['teams'])
    for col in df.columns:
        new_col_name = rename_camel_col(col)
        df.rename(columns={col: new_col_name},inplace=True)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def referee(context) -> pd.DataFrame:
    matches_df = context.resources.new_io_manager.load_table(table_name='matches')
    referees = matches_df['referee_i_d'].unique()
    frames = []
    for i in referees:
        try:
            referee_df = tm_fetch_referees(i)
            frames.append(referee_df)
        except Exception:
            context.log.info(f"Error with referee {i}")
    df = pd.concat(frames)
    df.rename(columns={'personID': 'person_id'},inplace=True)
    df.rename(columns={'clubID': 'club_id'},inplace=True)
    df.rename(columns={'countryID': 'country_id'},inplace=True)
    for col in df.columns:
        new_col_name = rename_camel_col(col)
        df.rename(columns={col: new_col_name},inplace=True)
    return df

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def staff_achievements(context) -> pd.DataFrame:
    staff_df = context.resources.new_io_manager.load_table(table_name='stuff')
    staff = staff_df['id'].unique()
    frames = []
    for i in staff:
        try:
            ndf = tm_fetch_staff_achievements(i)
            ndf['staff_id'] = i
            frames.append(ndf)
        except Exception:
            context.log.info(f"Error with staff {i}")
    df = pd.concat(frames)
    df.rename(columns={'achievementID': 'achievement_id'},inplace=True)
    df.rename(columns={'clubID': 'club_id'},inplace=True)
    df.rename(columns={'countryID': 'country_id'},inplace=True)
    df.rename(columns={'seasonID': 'season_id'},inplace=True)
    df.rename(columns={'competitionID': 'competition_id'},inplace=True)
    df.rename(columns={'roleID': 'role_id'},inplace=True)
    for col in df.columns:
        new_col_name = rename_camel_col(col)
        df.rename(columns={col: new_col_name},inplace=True)
    df['id'] = df.apply(lambda df: eval(f"{df['season_id']}{df['club_id']}"),axis=1)
    return df

# @asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
# def player_images(context) -> pd.DataFrame:
#     players_df = context.resources.new_io_manager.load_table(table_name='player')
#     players = players_df['id'].unique()
#     chunk_size = 60
#     frames = []
#     for chunk in chunk_list(players, chunk_size):
#         player_ids = ",".join(chunk)
#         url = f"{tm_url}players/images"
#         params = {"player_ids":player_ids,"locale":"US"}
#         response = tm_api_call(url, params)
#         try:
#             df = pd.DataFrame(response.json()['data'])
#             frames.append(df)
#         except Exception:
#             context.log.info(f"Error with chunk {player_ids}")
#     return pd.concat(frames)

@asset(group_name="ingest_v2", compute_kind="pandas", io_manager_key="new_io_manager")
def player_images(context) -> pd.DataFrame:
    player_ids = "282820,282821,319817,33952,39909,43261,214113,3163,61637,75287,184892,33951,38388,41355,186790,45272,3876,9619,32617,37397,258920,285847,4380,57515,63824,127987,164771,73472,241229,110798,128911,3924,245585,3476,7767,21725,28396,82215,46104,124845,88262,166640,167254,177952,115507,3109,4042,143559,182579,4624,195605,265763,15185,26763,226964,284717,7337,29356,70245,89648"
    url = f"{tm_url}players/images"
    params = {"player_ids":player_ids,"locale":"US"}
    response = tm_api_call(url, params)
    try:
        df = pd.DataFrame(response.json()['data'])
    except Exception:
        context.log.info(f"Error with chunk {player_ids}")
    return df

