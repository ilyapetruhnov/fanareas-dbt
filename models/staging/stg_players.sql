with players as (

    select * from {{ ref('raw_players') }}

),

continents as (

    select * from {{ ref('raw_continents') }}

),

countries as (

    select * from {{ ref('raw_countries') }}

),

cities as (

    select * from {{ ref('raw_cities') }}

),

types as (

    select * from {{ ref('raw_types') }}

),

squads as (

    select * from {{ ref('raw_squads') }}

),

seasons as (

    select * from {{ ref('raw_seasons') }}

),

teams as (

    select * from {{ ref('raw_teams') }}

),

countries_continents as (

    select
        countries.id as country_id,
        countries.name as country,
        continents.name as continent
    from countries
    left join continents
    on countries.continent_id = continents.id
),

squads_seasons_teams as (

    select
        player_id,
        squads.jersey_number,
        teams.id as team_id,
        teams.name as team,
        seasons.id as season_id,
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
        jersey_number,
        countries_continents.country_id as nationality_id,
        countries_continents.continent as continent,
        countries_continents.country as nationality,
        cities.name as city,
        types.id as position_id,
        types.name as position,
        squads_seasons_teams.team_id as team_id,
        squads_seasons_teams.team as team,
        squads_seasons_teams.season_id as season_id,
        squads_seasons_teams.season,
        players.name as fullname,
        players.image_path as image_path,
        firstname,
        lastname,
        height,
        weight,
        date_of_birth,
        row_number() over (partition by player_id, team_id, season_id order by player_id, team_id, season_id ) as rn
        from players
        left join countries_continents
        on players.nationality_id = countries_continents.country_id
        left join cities
        on players.city_id = cities.id
        left join types
        on detailed_position_id = types.id
        left join squads_seasons_teams
        on squads_seasons_teams.player_id = players.id
)

select
        player_id,
        jersey_number,
        nationality_id,
        continent,
        nationality,
        city,
        position_id,
        position,
        team_id,
        team,
        season_id,
        season,
        fullname,
        firstname,
        lastname,
        image_path,
        height,
        weight,
        date_of_birth
    from final
    where rn = 1
