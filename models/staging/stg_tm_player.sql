with player as (

    select *,
    row_number() over (partition by id order by id) as rn
    from {{ ref('tm_player') }}

),
final as (
select *
    from player
    where rn = 1
)
select * from final
