with types as (

    select * from {{ ref('raw_types') }}

),

player_stats as (

    select * from {{ ref('raw_player_stats') }}

),

player_stats_detailed as (

    select * from {{ ref('raw_player_stats_detailed') }}

),

final as (

select 
    distinct player_stats_detailed.id,
    player_statistic_id,
    player_id,
    team_id,
    season_id,
    position_id,
    jersey_number,
    types.name as type,
    total,
    goals,
    penalties,
    home,
    away
from player_stats_detailed
left join types on player_stats_detailed.type_id = types.id
left join player_stats on player_stats_detailed.player_statistic_id = player_stats.id
)

select * from final