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

final as (
    select
        agg_player_stats.player_id,
        firstname,
        lastname,
        date_of_birth,
        continent,
        nationality,
        team_id,
        team,
        substr(season, 1, 4) as season,
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
)

select * from final