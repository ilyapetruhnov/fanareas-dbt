with types as (

    select * from {{ ref('raw_types') }}

),

seasons as (

    select * from {{ ref('raw_seasons') }}

),

team_stats as (

    select * from {{ ref('raw_team_stats') }}

),

team_stats_detailed as (

    select * from {{ ref('raw_team_stats_detailed') }}

),

team_stats_seasons as (

    select 
        team_stats.id as team_stats_id,
        team_id,
        seasons.name as season_name
    from team_stats
    join seasons on seasons.id = team_stats.season_id

),

final as (

select 
    team_stats_seasons.team_stats_id,
    team_stats_seasons.team_id,
    team_stats_seasons.season_name,
    types.name as type,
    team_stats_detailed.*
from team_stats_detailed
join types on team_stats_detailed.type_id = types.id
join team_stats_seasons on team_stats_detailed.team_statistic_id = team_stats_seasons.team_stats_id
)

select * from final