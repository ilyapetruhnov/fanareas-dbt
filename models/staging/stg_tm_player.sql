with player as (

    select * from {{ ref('tm_player') }}

),
final as (
select *,   
    row_number() over (order by id) as rn
    from player
    where rn = 1
)
select * from final
