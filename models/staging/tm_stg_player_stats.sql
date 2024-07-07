with player_stats as (

    select * from {{ ref('player_performance') }}

),

player_stats_agg as (
    select 
    id as match_id,
    player_id,
    season_id,
    goals,
    assists,
    own_goals,
    minutes_played,
    case when minutes_played != 0 and substituted_on =0 then 1 else 0 end as lineups,
    case when minutes_played != 0 then 1 else 0 end as appearances,
    case when substituted_on >0 then 1 else 0 end as matches_coming_off_the_bench,
    case when substituted_off >0 then 1 else 0 end as matches_substituted_off,
    case when yellow_card_minute >0 then 1 else 0 end as yellow_cards,
    case when yellow_red_card_minute >0 then 1 else 0 end as second_yellow_cards,
    case when red_card_minute >0 then 1 else 0 end as red_cards
from player_stats

),

final as (
    select 
    player_id,
    season_id,
    sum(goals) as goals,
    sum(assists) as assists,
    sum(own_goals) as own_goals,
    sum(minutes_played) as minutes_played,
    sum(appearances) as appearances,
    sum(lineups) as lineups,
    sum(matches_coming_off_the_bench) as matches_coming_off_the_bench,
    sum(matches_substituted_off) as matches_substituted_off,
    sum(yellow_cards) as yellow_cards,
    sum(second_yellow_cards) as second_yellow_cards,
    sum(red_cards) as red_cards
from player_stats_agg
group by player_id, season_id

)

select * from final