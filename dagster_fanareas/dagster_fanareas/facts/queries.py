top_players_query = """
                with vw as (
                SELECT
                        player_id,
                        fullname,
                        array_to_string(team, '/') as team,
                        t.season_name,
                        t.assists as assists,
                        t.goals as goals,
                        t.red_cards as red_cards,
                        t.yellow_cards as yellow_cards,
                        t.penalties as penalties
                        FROM
                        dim_players
                        CROSS JOIN UNNEST (season_stats) AS t
                        WHERE
                        current_season = 2023
                        AND
                        t.season = {}
                ),
                vw1 as (
                select *
                            , row_number() over (ORDER BY assists desc nulls last)      as assists_rn
                            , row_number() over (ORDER BY goals desc nulls last)        as goals_rn
                            , row_number() over (ORDER BY red_cards desc nulls last)    as red_cards_rn
                            , row_number() over (ORDER BY yellow_cards desc nulls last) as yellow_cards_rn
                            , row_number() over (ORDER BY penalties desc nulls last) as as penalties_rn
                        from vw
                        )
                select *
                from vw1
                where assists_rn <= 10
                or goals_rn <= 10
                or red_cards_rn <= 10
                or yellow_cards_rn <= 10
                or penalties_rn <= 10
        """


top_teams_query = """
                with vw as (
                SELECT
                        player_id,
                        fullname,
                        array_to_string(team, '/') as team,
                        t.season_name,
                        t.assists as assists,
                        t.goals as goals,
                        t.red_cards as red_cards,
                        t.yellow_cards as yellow_cards,
                        t.minutes_played as minutes_played,
                        t.penalties as penalties
                        FROM
                        dim_players
                        CROSS JOIN UNNEST (season_stats) AS t
                        WHERE
                        current_season = 2023
                        AND
                        t.season = {}
                ),
                vw1 as (
                select *
                            , row_number() over (partition by team ORDER BY assists desc nulls last)      as assists_rn
                            , row_number() over (partition by team ORDER BY goals desc nulls last)        as goals_rn
                            , row_number() over (partition by team ORDER BY red_cards desc nulls last)    as red_cards_rn
                            , row_number() over (partition by team ORDER BY yellow_cards desc nulls last) as yellow_cards_rn
                            , row_number() over (partition by team ORDER BY penalties desc nulls last)    as penalties_rn
                        from vw
                        )
                select *
                from vw1
                where
                team not LIKE '%/%'
                AND (goals_rn <= 5
                or minutes_played_rn <= 5
                or assists_rn <= 5
                or red_cards_rn <= 5
                or yellow_cards_rn <= 5
                or penalties_rn <=5)
    """