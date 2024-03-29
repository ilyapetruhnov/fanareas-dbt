query_player_shirt_number="""
        SELECT
        firstname,
        lastname,
        fullname,
        array_to_string(team_id, '/') as team_id,
        array_to_string(team, '/') as team,
        array_to_string(jersey_number, '/') as jersey_number,
        t.season
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and array_length(t.team,1) = 1
        and is_active = true
        """

statement_player_shirt_number = "Which player currently plays for {} under {} jersey number?"


query_team_player_shirt_number="""
with vw as (SELECT firstname,
                   lastname,
                   fullname,
                   cast(array_to_string(team_id, '/') as int)       as team_id,
                   array_to_string(team, '/')          as team,
                   array_to_string(jersey_number, '/') as jersey_number,
                   t.season
            FROM dim_players
                     CROSS JOIN UNNEST(season_stats) AS t
            WHERE t.season = 2023
              and array_length(t.team, 1) = 1
              and is_active = true)
select * from vw
where team_id = {}"""

statement_team_player_shirt_number = "Which {0} player currently plays under {2} jersey number?"


query_player_age_nationality="""
        with vw as (
        SELECT
        firstname,
        lastname,
        fullname,
        nationality,
        date_of_birth,
        array_to_string(team, '/') as team,
        t.season,
        lead(nationality, 1) over (order by nationality, date_of_birth) as next_nationality,
        lead(date_of_birth, 1) over (order by nationality, date_of_birth) as next_date_of_birth
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and date_of_birth is not null
        and array_length(t.team,1) = 1
        and is_active = true),
        window_vw as (
        select
        fullname,
        nationality,
        next_nationality,
        cast(date_part('year', cast(date_of_birth as date)) as int) as birth_year,
        cast(date_part('year', cast(next_date_of_birth as date)) as int) as next_birth_year
        from vw)
        select * 
        from window_vw
        where
        next_nationality != nationality
        or next_birth_year != birth_year
        """

statement_player_age_nationality = "Which Premier League player was born in {} and is a citizen of {}?"


query_team_player_age_nationality = """
        with vw as (
        SELECT
        firstname,
        lastname,
        fullname,
        nationality,
        date_of_birth,
        cast(array_to_string(team_id, '/') as int)       as team_id,
        array_to_string(team, '/') as team,
        t.season,
        lead(nationality, 1) over (order by nationality, date_of_birth) as next_nationality,
        lead(date_of_birth, 1) over (order by nationality, date_of_birth) as next_date_of_birth
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and date_of_birth is not null
        and array_length(t.team,1) = 1
        and is_active = true),
        window_vw as (
        select
        fullname,
        nationality,
        next_nationality,
        team_id,
        cast(date_part('year', cast(date_of_birth as date)) as int) as birth_year,
        cast(date_part('year', cast(next_date_of_birth as date)) as int) as next_birth_year
        from vw)
        select * 
        from window_vw
        where
        (next_nationality != nationality
        or next_birth_year != birth_year)
        and team_id = {}
"""

statement_team_player_age_nationality = "Which {0} player was born in {1} and is a citizen of {2}?"


query_player_age_team="""
            with vw as (
            SELECT
            firstname,
            lastname,
            fullname,
            nationality,
            date_of_birth,
            array_to_string(team, '/') as team,
            t.season,
            lead(array_to_string(team, '/'), 1) over (order by array_to_string(team, '/'), date_of_birth) as next_team,
            lead(date_of_birth, 1) over (order by array_to_string(team, '/'), date_of_birth) as next_date_of_birth
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            t.season = 2023
            and date_of_birth is not null
            and array_length(t.team,1) = 1
            and is_active = true),
            window_vw as (
            select
            fullname,
            team,
            next_team,
            cast(date_part('year', cast(date_of_birth as date)) as int) as birth_year,
            cast(date_part('year', cast(next_date_of_birth as date)) as int) as next_birth_year
            from vw)
            select * 
            from window_vw
            where
            next_team != team
            or next_birth_year != birth_year
            """
statement_player_age_team = "Which player currently plays for {0} and was born in {2}?"


query_team_player_age = """
            with vw as (
            SELECT
            firstname,
            lastname,
            fullname,
            nationality,
            date_of_birth,
            cast(array_to_string(team_id, '/') as int)       as team_id,
            array_to_string(team, '/') as team,
            t.season,
            lead(array_to_string(team, '/'), 1) over (order by array_to_string(team, '/'), date_of_birth) as next_team,
            lead(date_of_birth, 1) over (order by array_to_string(team, '/'), date_of_birth) as next_date_of_birth
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            t.season = 2023
            and date_of_birth is not null
            and array_length(t.team,1) = 1
            and is_active = true),
            window_vw as (
            select
            fullname,
            team_id,
            team,
            next_team,
            cast(date_part('year', cast(date_of_birth as date)) as int) as birth_year,
            cast(date_part('year', cast(next_date_of_birth as date)) as int) as next_birth_year
            from vw)
            select fullname, team_id, team, birth_year
            from window_vw
            where
            (next_team != team
            or next_birth_year != birth_year)
            and team_id = {}
"""

statement_team_player_age_team = "Which {0} player was born in {2}?"



query_player_2_clubs_played="""
            WITH vw as (
            SELECT
            player_id,
            firstname,
            lastname,
            fullname,
            lag(array_to_string(team, '/')) over
                (partition by player_id order by t.season) transfer_from_team,
            array_to_string(team, '/') as team,
            array_to_string(jersey_number, '/') as jersey_number,
            team as team_arr,
            t.season as season,
            t.season_name
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            current_season = 2023
            AND array_length(team, 1) = 1
            ), window_vw as (
            SELECT
            *
            ,lead(transfer_from_team, 1) over (order by team, transfer_from_team, season_name) as next_transfer_from_team
            ,lead(team, 1) over (order by team, transfer_from_team, season_name) as next_team
            ,lead(season_name, 1) over (order by team, transfer_from_team, season_name) as next_season_name
            from vw)
            SELECT
                player_id,
                fullname,
                transfer_from_team,
                team,
                season,
                season_name
                from window_vw
                        where
                        team != transfer_from_team
                        AND
                (next_transfer_from_team != transfer_from_team
                or next_team != team
                or next_season_name != season_name
                )
        """
statement_player_2_clubs_played = "Which player played for {} and {} in his career?"


query_team_player_club_transferred_from = """
            WITH vw as (
            SELECT
            player_id,
            firstname,
            lastname,
            fullname,
            cast(array_to_string(team_id, '/') as int)       as team_id,
            lag(array_to_string(team, '/')) over
                (partition by player_id order by t.season) transfer_from_team,
            array_to_string(team, '/') as team,
            array_to_string(jersey_number, '/') as jersey_number,
            team as team_arr,
            t.season as season,
            t.season_name
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            current_season = 2023
            AND array_length(team, 1) = 1
            ), window_vw as (
            SELECT
            *
            ,lead(transfer_from_team, 1) over (order by team, transfer_from_team, season_name) as next_transfer_from_team
            ,lead(team, 1) over (order by team, transfer_from_team, season_name) as next_team
            ,lead(season_name, 1) over (order by team, transfer_from_team, season_name) as next_season_name
            from vw)
            SELECT
                player_id,
                fullname,
                team_id,
                transfer_from_team,
                team,
                season,
                season_name
                from window_vw
                        where
                        team != transfer_from_team
                        AND
                (next_transfer_from_team != transfer_from_team
                or next_team != team
                or next_season_name != season_name
                )
            AND team_id = {}
"""

statement_team_player_club_transferred_from = "Which player played for {1} before joining {0} in {2} season?"


query_player_transferred_from_to="""
            WITH vw as (
            SELECT
            player_id,
            firstname,
            lastname,
            fullname,
            lag(array_to_string(team, '/')) over
                (partition by player_id order by t.season) transfer_from_team,
            array_to_string(team, '/') as team,
            array_to_string(jersey_number, '/') as jersey_number,
            team as team_arr,
            t.season as season,
            t.season_name
            FROM
            dim_players
            CROSS JOIN UNNEST (season_stats) AS t
            WHERE
            current_season = 2023
            AND array_length(team, 1) = 1
            ), window_vw as (
            SELECT
            *
            ,lead(transfer_from_team, 1) over (order by team, transfer_from_team, season_name) as next_transfer_from_team
            ,lead(team, 1) over (order by team, transfer_from_team, season_name) as next_team
            ,lead(season_name, 1) over (order by team, transfer_from_team, season_name) as next_season_name
            from vw)
            SELECT
                player_id,
                fullname,
                transfer_from_team,
                team,
                season,
                season_name
                from window_vw
                        where
                        team != transfer_from_team
                        AND
                (next_transfer_from_team != transfer_from_team
                or next_team != team
                or next_season_name != season_name
                )
        """
statement_player_transferred_from_to = "Which player had a transfer from {} to {} in the {} season?"




query_player_height=f"""
        with vw as (
                    SELECT
                    height,
                    array_agg(fullname ORDER BY random()) as fullname
                    FROM
                    dim_players
                    where height is not null
                    and height != 0
                    and is_active = true
        group by height
        )
        select
        height,
        fullname[1] as fullname
        from vw
        """

statement_player_height = "Guess the tallest player from the following players"


query_team_player_position = """
with vw as (
SELECT
fullname,
        cast(array_to_string(team_id, '/') as int) as teamid,
        array_to_string(team, '/') as team_name,
        array_to_string(jersey_number, '/') as jersey_number,
        t.*
        FROM
        dim_players
        CROSS JOIN UNNEST (season_stats) AS t
        WHERE
        t.season = 2023
        and array_length(t.team,1) = 1
        ),
vw1 as (
SELECT position, season, array_agg(fullname ORDER BY random()) as players
FROM vw
where teamid = {}
and position is not null
group by position, season)
select position, season, players[1] as fullname from vw1
"""

statement_team_player_position = "Which {0} player currently plays at {1} position?"



query_team_stats = """with vw as (
select
team
,season
,team_lost_count as losses_cnt
,team_wins_count as wins_cnt
,team_draws_count as draws_cnt
,goals_all_count as goals_cnt
,goals_conceded_all_count as goals_conceded_cnt
,goals_all_count - goals_conceded_all_count as goal_difference_cnt
,yellowcards_count as yellow_cards_cnt
,redcards_count as red_cards_cnt
,cleansheets_count as clean_sheets_cnt
,corners_count as corners_cnt
, dense_rank() over (partition by season ORDER BY team_lost_count desc nulls last) as losses_rn
, dense_rank() over (partition by season ORDER BY team_wins_count desc nulls last) as wins_rn
, dense_rank() over (partition by season ORDER BY team_draws_count desc nulls last) as draws_rn
, dense_rank() over (partition by season ORDER BY goals_all_count desc nulls last) as goals_rn
, dense_rank() over (partition by season ORDER BY goals_conceded_all_count desc nulls last) as goals_conceded_rn
, dense_rank() over (partition by season ORDER BY (goals_all_count - goals_conceded_all_count) desc nulls last) as goal_difference_rn
, dense_rank() over (partition by season ORDER BY yellowcards_count desc nulls last) as yellow_cards_rn
, dense_rank() over (partition by season ORDER BY redcards_count desc nulls last) as red_cards_rn
, dense_rank() over (partition by season ORDER BY cleansheets_count desc nulls last) as clean_sheets_rn
, dense_rank() over (partition by season ORDER BY corners_count desc nulls last) as corners_rn
from dim_team_stats
where season not in ('2023/2024', '2018/2019','2019/2020')
)
select
    team
    ,season
     ,losses_cnt
     ,losses_rn
     ,wins_cnt
     ,wins_rn
     ,draws_cnt
     ,draws_rn
     ,goals_cnt
     ,goals_rn
     ,goals_conceded_cnt
     ,goals_conceded_rn
     ,goal_difference_cnt
     ,goal_difference_rn
     ,yellow_cards_cnt
     ,yellow_cards_rn
     ,red_cards_cnt
     ,red_cards_rn
     ,clean_sheets_cnt
     ,clean_sheets_rn
     ,corners_cnt
     ,corners_rn
from vw
"""

statement_team_stats = "Which team had the most {} in the {} season?"

query_capacity_venue = """select *,
dense_rank() over (order by capacity desc) as capacity_rn,
dense_rank() over (order by founded asc) as founded_rn
from stg_teams"""


query_standings = """
select * from stg_standings
where season != '2023/2024' 
order by points
"""


query_relegations = """
with vw as (select season,
                   array_agg(team) as teams
            from stg_standings
            where season not in ('2023/2024', '2005/2006')
            group by season),
    vw1 as (
select season,
       teams,
       lag(teams, 1) over (order by season) as prev_teams,
       lead(teams, 1) over (order by season) as next_teams
from vw),
vw2 as (
select *,
array(select unnest(next_teams) except select unnest(teams)) as teams_promoted,
array(select unnest(teams) except select unnest(next_teams)) as teams_relegated
from vw1)
select season,
       teams_promoted[1] as team_promoted,
       (teams_promoted[1] || teams_relegated) as options
from vw2
"""