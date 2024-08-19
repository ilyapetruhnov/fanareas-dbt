national_champions_query = """with vw as (
                            select country_name, coach_name, cast(season_id as int) as season_id, cast(season as int) as season, 'UEFA European Championship' as title from euro_champions
                            union select country_name, coach_name, cast(season_id as int) as season_id, cast(season as int) as season, 'FIFA World Cup' as title from world_champions
    )
                            select *
                            from vw
                            where title = '{}'
                            order by season
                            """

both_national_cups_query = """with vw as (
                            select country_name, success_id, cast(season_id as int) as season_id, cast(season as int) as season, 'UEFA European Championship' as title from euro_champions
                            union select country_name, success_id, cast(season_id as int) as season_id, cast(season as int) as season, 'FIFA World Cup' as title from world_champions
    )
                            select *
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


top_value_players_query = """select * from tm_dim_top_players_by_value order by tm_market_value desc limit 450"""

played_for_multiple_clubs_query = """select * from played_for_multiple_clubs"""

player_transfers_over_fifty_million_query = """select * from dim_player_transfers where international_team != '' and transfer_fee_value > 49999999"""


own_goals_query = """select * from tm_dim_top_league_players
                        where own_goals > goals
                        and goals >0
                        order by own_goals desc
                        limit 5"""

own_goals_options_query = """
select * from tm_dim_top_league_players
                        where goals > own_goals
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
                            select player_id, fullname, league_name,
                                   cast(sum(red_cards) as int) as red_cards,
                                   cast(sum(combined_cards) as int) as combined_cards
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
                            and fullname != 'Andreas Köpke'
                            group by fullname, nationality
                            order by goals desc"""

other_goalkeepers_query = """
                        SELECT distinct fullname from tm_dim_goalkeepers_with_goals
                        where goals = 0
                        and fullname != '{}'
                        limit 20"""

standing_query = """select * from standing where season_id = {} and league_id = '{}'"""

player_for_the_team_query = """select * from tm_dim_top_players_by_value
                                where international_team != ''
                                order by tm_market_value desc
                                """

most_titles_won_query = """with titles as (
select team_id, count(id ) as title_cnt
from titles
where competition_id = '{}'
group by team_id),
teams as (select id, name from tm_team where league_id = '{}')
select name, case when title_cnt is null then 0 else title_cnt end as title_cnt from titles
right join teams
on teams.id = titles.team_id
order by title_cnt desc"""

cl_titles_query = """with vw as (
                    select
                    t.name as team_name,
                    t.league_id,
                    t.league_name,
                    titles.name title,
                    case when titles.id in ('4','55') then 'CL'
                        when titles.id in ('12','11','10','13','14') then 'League'
                        when titles.id in ('29','27','35','96','94') then 'Cup'
                        when titles.id in ('7','264') then 'EL'
                        else 'unknown' end as title_name,
                    titles.id as title_id,
                    cast(max(number) as int) as number
                    from titles
                    full outer join team t on titles.team_id = t.id
                    group by t.name,
                    t.league_id,
                    t.league_name,
                    titles.id,
                    titles.name),
                    title_vw as (
                    select team_name,
                        league_name,
                        title_name,
                        sum(number) as number
                    from vw
                    group by team_name, league_name, title_name)
                    SELECT *
                    from title_vw
                    WHERE title_name = 'CL'
                    order by number desc
                    """


el_titles_query = """with vw as (
                    select
                    t.name as team_name,
                    t.league_id,
                    t.league_name,
                    titles.name title,
                    case when titles.id in ('4','55') then 'CL'
                        when titles.id in ('12','11','10','13','14') then 'League'
                        when titles.id in ('29','27','35','96','94') then 'Cup'
                        when titles.id in ('7','264') then 'EL'
                        else 'unknown' end as title_name,
                    titles.id as title_id,
                    cast(max(number) as int) as number
                    from titles
                    full outer join team t on titles.team_id = t.id
                    group by t.name,
                    t.league_id,
                    t.league_name,
                    titles.id,
                    titles.name),
                    title_vw as (
                    select team_name,
                        league_name,
                        title_name,
                        sum(number) as number
                    from vw
                    group by team_name, league_name, title_name)
                    SELECT *
                    from title_vw
                    WHERE title_name = 'EL'
                    order by number desc
                    """

cup_titles_query = """with vw as (
                    select
                    t.name as team_name,
                    t.league_id,
                    t.league_name,
                    titles.name title,
                    case when titles.id in ('4','55') then 'CL'
                        when titles.id in ('12','11','10','13','14') then 'League'
                        when titles.id in ('29','27','35','96','94') then 'Cup'
                        when titles.id in ('7','264') then 'EL'
                        else 'unknown' end as title_name,
                    titles.id as title_id,
                    cast(max(number) as int) as number
                    from titles
                    full outer join team t on titles.team_id = t.id
                    group by t.name,
                    t.league_id,
                    t.league_name,
                    titles.id,
                    titles.name),
                    title_vw as (
                    select team_name,
                        league_name,
                        title_name,
                        title,
                        sum(number) as number
                    from vw
                    group by team_name, league_name, title_name, title)
                    SELECT *
                    from title_vw
                    WHERE title_name = 'Cup'
                    and league_name = '{}'
                    order by number desc
                    """

team_coach_query = """
select coach_name, league_name, tm_team.name as team
from tm_team
join standing
on cast(standing.team_id as int) = cast(tm_team.id as int)
where league_name = '{}'
and rank < 6
and season_id = 2023
"""

team_stats_query = """with vw as (select team_id,
       sum(cast(offsides as int)) as offsides,
       avg(cast(ballpossession as float)) as avg_possession,
       sum(cast(fouls as int)) as fouls,
       sum(cast(cornerkicks as int)) as cornerkicks
from match_stats
join matches on match_stats.match_id = matches.id
where season_i_d = '2023'
group by team_id)
select league_name, name as team_name, offsides,
       avg_possession,
       fouls,
       cornerkicks
from vw
join tm_team
on tm_team.id = vw.team_id
where league_name = '{}'"""


most_conceded_goals_query = """
                            select
                            club_name,
                            goals_conceded,
                            season_id,
                            league_id
                            from standing
                            where league_id = '{}'
                            order by goals_conceded desc
                            """

most_scored_goals_query = """
                            select
                            club_name,
                            goals,
                            season_id,
                            league_id
                            from standing
                            where league_id = '{}'
                            order by goals desc
                            """

points_query = """
                            select
                            club_name,
                            goals,
                            points,
                            season_id,
                            league_id
                            from standing
                            where league_id = '{}'
                            and season_id != 2024 
                            """

unbeaten_query = """select
season_id,
club_name,
wins,
draw,
tm_team.league_id
from standing
join tm_team
on cast(standing.team_id as int) = cast(tm_team.id as int)
where season_id < 2024
and losses = 0
and tm_team.league_id = '{}'
"""

unbeaten_options_query = """
select club_name from standing where season_id = 2023 and league_id = '{}'
and rank < 6
and club_name != '{}'
"""

team_logo_options_query = """
                    select
                    tm_team.name
                    from standing
                    join tm_team
                    on cast(standing.team_id as int) = cast(tm_team.id as int)
                    where season_id = 2023
                    and tm_team.league_id not in ('FR1','FR2','L2','GB2','IT2')
                    """

logo_select_query = """
select image from tm_team where name = '{}'
"""

won_league_with_fewest_points_query = """
select standing.points, standing.season_id, tm_team.* from standing
                                  join tm_team
                                  on cast(tm_team.id as int) = standing.team_id
where standing.rank = 1
and standing.matches = 38
and standing.league_id = '{}'
and standing.season_id < 2024
order by standing.points
"""