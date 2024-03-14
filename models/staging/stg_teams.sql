with countries as (

    select * from {{ ref('raw_countries') }}

),

venues as (

    select * from {{ ref('raw_venues') }}

),

teams as (

    select * from {{ ref('raw_teams') }}

),

final as (

    select
        teams.id as team_id,
        teams.country_id,
        teams.name as team,
        teams.short_code as short_code,
        teams.founded as founded,
        countries.name as country,
        venues.name as venue,
        venues.capacity as capacity,
        venues.city_name as city
    from teams
    left join countries
    on teams.country_id  = countries.id
    left join venues
    on teams.venue_id  = venues.id
)

select * from final