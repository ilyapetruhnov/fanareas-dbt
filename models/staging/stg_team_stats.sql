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
    left join seasons on seasons.id = team_stats.season_id

),

final as (

select 
    team_stats_seasons.team_stats_id,
    team_stats_seasons.team_id,
    team_stats_seasons.season_name,
    types.name as type,
    *
from team_stats_detailed
left join types on team_stats_detailed.type_id = types.id
left join team_stats_seasons on team_stats_detailed.team_statistic_id = team_stats_seasons.id
)

select * from final