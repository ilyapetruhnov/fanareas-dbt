national_champions_query = """with vw as (
                            select *, 'UEFA European Championship' as title from euro_champions
                            union select *, 'FIFA World Cup' as title from world_champions)
                            select *, 
                            cast(season as int) as season,
                            cast(season_id as int) as season_id
                            from vw
                            where title = '{}'
                            order by season
                            """

both_national_cups_query = """with vw as (
                            select *, 'UEFA European Championship' as title from euro_champions
                            union select *, 'FIFA World Cup' as title from world_champions)
                            select *, 
                            cast(season as int) as season,
                            cast(season_id as int) as season_id
                            from vw
                            order by season
                            """

team_query = """
                select 
                    name as team_name,
                    league_name,
                    city, 
                    stadium_name,
                    stadium_image,
                    construction_year,
                    cast(total_capacity as int) as  total_capacity
                from team
                where (stadium_image <> '') IS NOT FALSE
                and (stadium_name <> '') IS NOT FALSE
                order by total_capacity desc
                """

player_query = """select * from tm_stg_player"""


top_value_players_query = """select * from tm_dim_top_players_by_value limit 300"""