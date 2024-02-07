with source as (

    select * from {{ source('src_postgres', 'squads') }}
),
renamed as (

    select 
    id,
    player_id,
    team_id
    from source
)

select * from renamed