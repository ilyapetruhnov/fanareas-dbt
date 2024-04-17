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
                            , row_number() over (ORDER BY penalties desc nulls last)    as penalties_rn
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
                        team,
                        season_name,
                        assists,
                        goals,
                        red_cards,
                        yellow_cards,
                        minutes_played,
                        penalties,
                        own_goals
                        FROM
                        dim_player_team_stats
                        WHERE
                        season = {}
                ),
                vw1 as (
                select *
                            , row_number() over (partition by team ORDER BY assists desc nulls last)      as assists_rn
                            , row_number() over (partition by team ORDER BY goals desc nulls last)        as goals_rn
                            , row_number() over (partition by team ORDER BY red_cards desc nulls last)    as red_cards_rn
                            , row_number() over (partition by team ORDER BY yellow_cards desc nulls last) as yellow_cards_rn
                            , row_number() over (partition by team ORDER BY penalties desc nulls last)    as penalties_rn
                            , row_number() over (partition by team ORDER BY minutes_played desc nulls last)    as minutes_played_rn
                            , row_number() over (partition by team ORDER BY own_goals desc nulls last)    as own_goals_rn
                        from vw
                        )
                select *
                from vw1
                where
                goals_rn <= 5
                or minutes_played_rn <= 5
                or assists_rn <= 5
                or red_cards_rn <= 5
                or yellow_cards_rn <= 5
                or penalties_rn <=5
                or own_goals_rn <=5
    """

top_team_stats_query = """
                with vw as (
                SELECT
                        player_id,
                        fullname,
                        array_to_string(team, '/') as team,
                        t.season_name,
                        t.assists as assists,
                        t.goals as goals,
                        t.goals + t.assists as goals_assists,
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
                            , row_number() over (ORDER BY goals_assists desc nulls last) as goals_assists_rn
                            , row_number() over (ORDER BY yellow_cards desc nulls last) as yellow_cards_rn
                            , row_number() over (ORDER BY penalties desc nulls last)    as penalties_rn
                        from vw
                        )
                select *
                from vw1
                where assists_rn <= 10
                or goals_rn <= 10
                or yellow_cards_rn <= 10
                or penalties_rn <= 10
                or goals_assists_rn <=10
        """