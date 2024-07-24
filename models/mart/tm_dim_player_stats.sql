with player as (

    select * from {{ ref('stg_tm_player') }}

),

squad as (

    select * from {{ ref('tm_squad') }}

),

team as (

    select * from {{ ref('tm_team') }}

),

player_performace as (

    select * from {{ ref('tm_player_performace') }}

),

stg_players as (

    select
        cast(player.id as int) as player_id,
        cast(squad.season_id as int),
        cast(squad.team_id as int),
        country as nationality,
        squad.age,
        player.international_team,
        position_group,
        player_main_position as position,
        squad.shirt_number as jersey_number,
        squad.joined,
        agent,
        player_name as fullname,
        player_image as image_path,
        height,
        foot,
        date_of_birth,
        squad.captain,
        squad.market_value,
        squad.market_value_currency
from player
join squad on player.id = squad.player_id
),
player_stats_agg as (
    select
    player_performace.id as match_id,
    player_id,
    season_id,
    team_id,
    team.name as team,
    team.league_name as league_name,
    goals,
    assists,
    own_goals,
    minutes_played,
    case when minutes_played != 0 and substituted_on =0 then 1 else 0 end as lineups,
    case when minutes_played != 0 then 1 else 0 end as appearances,
    case when substituted_on >0 then 1 else 0 end as matches_coming_off_the_bench,
    case when substituted_off >0 then 1 else 0 end as matches_substituted_off,
    case when yellow_card_minute >0 then 1 else 0 end as yellow_cards,
    case when yellow_red_card_minute >0 then 1 else 0 end as second_yellow_cards,
    case when red_card_minute >0 then 1 else 0 end as red_cards
from player_performace
join team on player_performace.team_id = cast(team.id as int)
),
stg_player_stats as (
    select
    player_id,
    season_id,
    team_id,
    team,
    league_name,
    sum(goals) as goals,
    sum(assists) as assists,
    sum(own_goals) as own_goals,
    sum(minutes_played) as minutes_played,
    sum(appearances) as appearances,
    sum(lineups) as lineups,
    sum(matches_coming_off_the_bench) as matches_coming_off_the_bench,
    sum(matches_substituted_off) as matches_substituted_off,
    sum(yellow_cards) as yellow_cards,
    sum(second_yellow_cards) as second_yellow_cards,
    sum(red_cards) as red_cards
from player_stats_agg
group by player_id, season_id, team_id, team, league_name
    )
select
    stg_players.player_id,
    stg_players.season_id,
    stg_players.nationality,
    stg_players.fullname,
    stg_players.captain,
    ARRAY_AGG(DISTINCT stg_player_stats.team_id) as team_id_arr,
    ARRAY_AGG(DISTINCT stg_player_stats.team) as team_arr,
    ARRAY_AGG(DISTINCT stg_player_stats.league_name) as league_name_arr,
    max(stg_players.position_group) as position_group,
    max(stg_players.position) as position,
    max(stg_players.international_team) as international_team,
    max(stg_players.image_path) as image_path,
    max(stg_players.market_value) as market_value,
    max(stg_players.market_value_currency) as market_value_currency,
    max(stg_players.jersey_number) as jersey_number,
    max(goals) as goals,
    max(assists) as assists,
    max(own_goals) as own_goals,
    max(minutes_played) as minutes_played,
    max(appearances) as appearances,
    max(lineups) as lineups,
    max(matches_coming_off_the_bench) as matches_coming_off_the_bench,
    max(matches_substituted_off) as matches_substituted_off,
    max(yellow_cards) as yellow_cards,
    max(second_yellow_cards) as second_yellow_cards,
    max(red_cards) as red_cards
from stg_players
join stg_player_stats
on stg_players.player_id = stg_player_stats.player_id
and stg_players.season_id = stg_player_stats.season_id
group by 
    stg_players.player_id,
    stg_players.season_id,
    stg_players.nationality,
    stg_players.fullname,
    stg_players.captain


