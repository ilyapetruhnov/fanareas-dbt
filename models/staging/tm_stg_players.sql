with players as (

    select * from {{ ref('player') }}

),

squads as (

    select * from {{ ref('squad') }}

),

teams as (

    select * from {{ ref('team') }}

),

final as (

    select
        player.id as player_id,
        squad.season_id,
        squad.team_id,
        team.name as team,
        country as nationality,
        squad.age,
        player.international_team,
        position_group,
        player_main_position as position,
        squad.shirt_number as jersey_number,
        squad.joined,
        agent,
        player_name as fullname,
        player_image as image_path,
        height,
        foot,
        date_of_birth,
        squad.captain,
        squad.market_value,
        squad.market_value_currency
from player
join squad on player.id = squad.player_id
join team on player.team_id = team.id
)

select * from final
