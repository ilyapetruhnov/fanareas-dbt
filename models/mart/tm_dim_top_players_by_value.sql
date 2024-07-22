with stg_tm_player as (

    select * from {{ ref('stg_tm_player') }}

),

vw as (

select *,
    REPLACE(market_value, '-', '0') as marketvalue,
    case
    when market_value_numeral = 'mil.' then 1000000
    when market_value_numeral = 'k' then 1000
    else 0
end as value_multiple
FROM stg_tm_player

),

final as (

select 
    id,
    player_image,
    img,
    player_name,
    birthplace_country,
    age,
    height,
    foot,
    international_team,
    country,
    second_country,
    league,
    team,
    team_id,
    position_group,
    player_main_position,
    (CAST(REPLACE(marketvalue, ',', '.') AS FLOAT) * value_multiple) as tm_market_value
FROM vw

)

select * 
from final
order by tm_market_value desc
limit 500