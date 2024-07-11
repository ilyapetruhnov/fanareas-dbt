with player as (

    select * from {{ ref('tm_player') }}

),

with squad as (

    select * from {{ ref('tm_squad') }}

),

with team as (

    select * from {{ ref('tm_team') }}

),

with player_performace as (

    select * from {{ ref('tm_player_performace') }}

),

stg_players as (

    select
        cast(player.id as int) as player_id,
        cast(squad.season_id as int),
        cast(squad.team_id as int),
        team.name as team,
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
join team on player.team_id = team.id
),
player_stats_agg as (
    select
    id as match_id,
    player_id,
    season_id,
    team_id,
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
),
stg_player_stats as (
    select
    player_id,
    season_id,
    team_id,
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
group by player_id, season_id, team_id
    )
select
    stg_players.player_id,
    stg_players.season_id,
    stg_player_stats.team_id,
    stg_players.nationality,
    stg_players.fullname,
    stg_players.position_group,
    stg_players.position,
    stg_players.international_team,
    stg_players.captain,
    stg_players.image_path,
    stg_players.market_value,
    stg_players.market_value_currency,
    stg_players.jersey_number,
    goals,
    assists,
    own_goals,
    minutes_played,
    appearances,
    lineups,
    matches_coming_off_the_bench,
    matches_substituted_off,
    yellow_cards,
    second_yellow_cards,
    red_cards
from stg_players
join stg_player_stats
on stg_players.player_id = stg_player_stats.player_id
and stg_players.season_id = stg_player_stats.season_id


