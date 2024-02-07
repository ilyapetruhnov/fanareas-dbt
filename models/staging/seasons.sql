with source as (

    select * from {{ source('src_postgres', 'seasons') }}
),
renamed as (

    select 
    id,
    name,
    finished
    from source
)

select * from renamed