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

played_for_multiple_clubs_query = """select * from played_for_multiple_clubs"""

player_transfers_over_million_query = """select * from  dim_player_transfers where international_team != '' and transfer_fee_value > 9999999"""


own_goals_query = """select * from tm_dim_top_league_players
                        where own_goals > goals
                        and league = '{}'
                        and goals >0
                        order by own_goals desc
                        limit 1"""

own_goals_options_query = """
select * from tm_dim_top_league_players
                        where goals > own_goals
                        and league = '{}'
                        and own_goals > 0
                        order by yellow_cards desc
                        limit 50
"""

player_position_performance_query = """select * from tm_dim_top_league_players
                                        where league = '{}'
                                        and position_group = '{}'
                                        order by {} desc
                                        limit 50"""

player_position_performance_options_query = """select * from tm_dim_top_league_players
                                                where league = '{}'
                                                and position_group = '{}'
                                                and appearances > 200
                                                order by {}
                                                limit 10
                                                """



most_stats_in_a_league_query = """select * from tm_dim_top_league_players
                                    where league = '{}' 
                                    order by {} desc"""


comparison_query = """with correct as (select * from tm_dim_top_league_players
                            where
                            league = '{}'
                            and {} > {}
                            ORDER BY RANDOM()
                            limit 1)
                            ,
                            options as (
                            select * from tm_dim_top_league_players
                            where
                            league = '{}'
                            and {}  < {}
                            and {}  > {}
                            ORDER BY RANDOM()
                            limit 3)
                            select * from correct
                            union
                            select * from options"""

cards_combined_query = """
                            with vw as (select *, (red_cards+second_yellow_cards+yellow_cards) as combined_cards
                                    from tm_dim_player_stats
                                    )
                            select player_id, fullname, league_name, cast(sum(combined_cards) as int) combined_cards
                            from vw
                            where league_name = '{}'
                            group by player_id, fullname, league_name
                            order by sum(combined_cards) desc
                            limit 4
                            """

played_in_4_major_leagues = """
with agg_vw as (
    select player_id,fullname,
    array_agg(distinct league_name) as played_leagues
    from tm_dim_player_stats
    group by player_id,fullname

),
    final_vw as (
                select *,
                array_length(played_leagues, 1) as leagues_len
                from agg_vw
)
                select fullname,
                       played_leagues,
                       leagues_len
                from final_vw
                where leagues_len > 4
                and '{}' = ANY(played_leagues)
                and '{}' = ANY(played_leagues)
                and '{}' = ANY(played_leagues)
                and '{}' = ANY(played_leagues)
                order by fullname desc
"""

played_in_less_4_leagues_query = """
with agg_vw as (
    select player_id,fullname,
           sum(appearances) as appearances,
    array_agg(distinct league_name) as played_leagues
    from tm_dim_player_stats
    group by player_id,fullname

),
    final_vw as (
                select *,
                array_length(played_leagues, 1) as leagues_len
                from agg_vw
                where appearances > 400
)
                select fullname,
                       appearances,
                       played_leagues,
                       leagues_len
                from final_vw
                where leagues_len < 4
                limit 50
"""

goalkeeper_stats_query = """SELECT fullname, nationality, sum(goals) as goals from tm_dim_goalkeepers_with_goals
                            where goals > 1
                            group by fullname, nationality"""

other_goalkeepers_query = """
                        SELECT distinct fullname from tm_dim_goalkeepers_with_goals
                        where goals = 0
                        and fullname != '{}'
                        and nationality = '{}'
                        limit 20"""