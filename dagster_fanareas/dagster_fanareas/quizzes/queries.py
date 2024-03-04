query_player_shirt_number="""
        SELECT
        firstname,
        lastname,
        fullname,
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

statement_player_age_nationality = "Which Premier League player was born in {} in {}?"

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
statement_player_age_team = "Which player currently plays for {} and was born in {}?"

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


statement_player_height = "Guess the tallest player from the following players"
# i = random.randint(0, 40)
query_player_height=f"""
            with vw as (
                    SELECT
                    height,
                    array_agg(fullname) as fullname
                    FROM
                    dim_players
                    where height is not null
                    and height != 0
                    and is_active = true
            group by height
            )
            select
            height,
            fullname[{0}] AS fullname
            from vw
        """