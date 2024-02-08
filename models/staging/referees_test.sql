with source as (

    select * from {{ source('src_postgres', 'referees') }}
),
renamed as (

    select 
    id,
    name
    from referees
)

select * from renamed