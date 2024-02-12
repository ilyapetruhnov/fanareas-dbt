with players as (
    select * from {{ ref('stg_players') }}
),

player_stats as (
    select * from {{ ref('stg_player_stats') }}

),

agg_player_stats as (

    select
        player_stats.player_id,
        player_stats.team_id,
        player_stats.season_id,
        max(jersey_number) as jersey_number,
        max(total) filter (where type = 'Captain') as captain,
        max(total) filter (where type = 'Yellowcards') as yellow_cards,
        max(total) filter (where type = 'Redcards') as red_cards,
        max(total) filter (where type = 'Yellowred Cards') as yellow_red_cards,
        max(total) filter (where type = 'Minutes Played') as minutes_played,
        max(total) filter (where type = 'Appearances') as appearances,
        max(total) filter (where type = 'Assists') as assists,
        max(total) filter (where type = 'Lineups') as lineups,
        max(total) filter (where type = 'Goals') as goals,
        max(home) as home,
        max(away) as away,
        max(penalties) as penalties,
        max(total) filter (where type = 'Own Goals') as own_goals,
        max(total) filter (where type = 'Goals Conceded') as goals_conceded
    from player_stats
    group by player_stats.player_id,
             player_stats.team_id,
             player_stats.season_id
),

joined as (
    select
        agg_player_stats.player_id,
        firstname,
        lastname,
        date_of_birth,
        continent,
        nationality,
        agg_player_stats.team_id as team_id,
        team,
        cast(substr(season, 1, 4) as int) as season,
        position,
        height,
        weight,
        jersey_number,
        captain,
        yellow_cards,
        red_cards,
        yellow_red_cards,
        minutes_played,
        appearances,
        assists,
        lineups,
        goals,
        home,
        away,
        penalties,
        own_goals,
        goals_conceded
    from agg_player_stats
    join players
    on players.season_id = agg_player_stats.season_id
    and players.team_id = agg_player_stats.team_id
    and players.player_id = agg_player_stats.player_id
),

final as (

SELECT
    (player_id + season) as pkey
    player_id,
    season,
    max(firstname) as firstname,
    max(lastname)  as lastname,
    max(date_of_birth)  as date_of_birth,
    max(continent) as continent,
    max(nationality) as nationality,
    array_agg(team_id) as team_id,
    array_agg(team) as team,
    array_agg(jersey_number) as jersey_number,
    max(height) as height,
    max(weight) as weight,
    sum(captain) as captain,
    sum(yellow_cards) as yellow_cards,
    sum(home) as home_yellow_cards,
    sum(away) as away_yellow_cards,
    sum(red_cards) as red_cards,
    sum(yellow_red_cards) as yellow_red_cards,
    sum(appearances) as appearances,
    sum(assists) as assists,
    sum(lineups) as lineups,
    sum(goals) as goals,
    sum(penalties) as penalties,
    sum(own_goals) as own_goals,
    sum(goals_conceded) as goals_conceded
FROM joined
group by player_id, season
)

select * from final