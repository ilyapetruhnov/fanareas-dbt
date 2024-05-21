with standings as (

    select * from {{ ref('raw_standings') }}

),

seasons as (

    select * from {{ ref('raw_seasons') }}

),

stg_teams as (

    select * from {{ ref('stg_teams') }}

),

final as (

    select
    stg_teams.team_id,
    stg_teams.team,
    seasons.name as season,
    standings.position,
    standings.points,
    stg_teams.founded as founded,
    stg_teams.venue as venue,
    stg_teams.capacity as capacity,
    stg_teams.city as city,
    row_number() over (partition by team_id, seasons.name order by team_id ) as rn
    from standings
    join seasons on standings.season_id = seasons.id
    join stg_teams on stg_teams.team_id = participant_id

)
    select
    team_id,
    team,
    season,
    position,
    points,
    founded,
    venue,
    capacity,
    city
    from final
    where rn = 1