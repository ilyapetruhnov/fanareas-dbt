with players as (

    select * from {{ source('src_postgres', 'players') }}

),

countries as (

    select * from {{ source('src_postgres', 'countries') }}

),

cities as (

    select * from {{ source('src_postgres', 'cities') }}

),

types as (

    select * from {{ source('src_postgres', 'types') }}

),

squads as (

    select * from {{ source('src_postgres', 'squads') }}

),

seasons as (

    select * from {{ source('src_postgres', 'seasons') }}

),

teams as (

    select * from {{ source('src_postgres', 'teams') }}

),

squads_seasons_teams as (

        select
        id as squad_id,
        player_id,
        teams.name as team,
        seasons.name as season
    from squads
    left join seasons
    on squads.season_id = seasons.id
    left join teams
    on squads.team_id = teams.id
),

final as (

select

    players.id as player_id,
    squad_id,
    countries.name as country,
    cities.name as city,
    types.name as position,
    squads_seasons_teams.name as team,
    squads_seasons_teams.season,
    firstname,
    lastname,
    height,
    weight,
    date_of_birth
    from players
    left join countries
    on country_id = countries.id
    left join cities
    on city_id = cities.id
    left join types
    on detailed_position_id = types.id
    left join squads_seasons_teams
    on squads_seasons_teams.player_id = players.id

)

select * from final







;