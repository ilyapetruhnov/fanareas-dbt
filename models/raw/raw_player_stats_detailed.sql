select * from {{ source('src_postgres', 'player_stats_detailed') }}