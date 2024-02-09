with players as (

    select * from {{ source('src_postgres', 'players') }}

),

continents as (

    select * from {{ source('src_postgres', 'continents') }}

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

countries_cities_continents as (

    select
        cities.id as city_id,
        cities.name as city,
        countries.id as country_id,
        countries.name as country,
        continents.name as continent
    from cities
    left join countries
    on cities.country_id = countries.id
    left join continents
    on countries.continent_id = continents.id
),

squads_seasons_teams as (

    select
        squads.id as squad_id,
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
    countries_cities_continents.country as country,
    countries_cities_continents.city as city,
    countries_cities_continents.continent as continent,
    types.name as position,
    squads_seasons_teams.team as team,
    squads_seasons_teams.season,
    firstname,
    lastname,
    height,
    weight,
    date_of_birth
    from players
    left join countries_cities_continents
    on players.city_id = countries_cities_continents.city_id
    left join types
    on detailed_position_id = types.id
    left join squads_seasons_teams
    on squads_seasons_teams.player_id = players.id
)

select * from final







;