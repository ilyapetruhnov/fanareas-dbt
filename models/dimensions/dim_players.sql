{{
    config(
        materialized='incremental',
        unique_key=['player_id', 'current_season'],
        incremental_strategy='insert_overwrite',
        on_schema_change = 'fail'
    )
}}
with
  last_season as (
    SELECT
      *
    FROM
      dim_players
    WHERE
      current_season = 2022
 ),
  this_season AS (
    SELECT
      *
    FROM
      {{ ref('dim_player_stats') }}
    WHERE
      season = 2023
  ),
  final AS (
SELECT
  COALESCE(ts.player_id, ls.player_id) AS player_id,
  COALESCE(ts.firstname, ls.firstname) AS firstname,
  COALESCE(ts.lastname, ls.lastname) AS lastname,
  COALESCE(ts.fullname, ls.fullname) AS fullname,
  COALESCE(ts.date_of_birth, ls.date_of_birth) AS date_of_birth,
  COALESCE(ts.continent, ls.continent) AS continent,
  COALESCE(ts.nationality, ls.nationality) AS nationality,
  COALESCE(ts.height, ls.height) AS height,
  COALESCE(ts.weight, ls.weight) AS weight,
  CASE
    WHEN ts.season IS NULL THEN ls.season_stats
    WHEN ts.season IS NOT NULL AND ls.season_stats IS NULL
        THEN
        ARRAY[
            ROW (
        ts.season,
        ts.season_name,
        ts.team,
        ts.team_id,
        ts.jersey_number,
        ts.position,
        ts.captain,
        ts.yellow_cards,
        ts.red_cards,
        ts.yellow_red_cards,
        ts.minutes_played,
        ts.appearances,
        ts.assists,
        ts.lineups,
        ts.goals,
        ts.home_yellow_cards,
        ts.away_yellow_cards,
        ts.penalties,
        ts.own_goals,
        ts.goals_conceded
      )::season_stats
     ]
    WHEN ts.season IS NOT NULL AND ls.season_stats IS NOT NULL
        THEN ARRAY_CAT(ls.season_stats,
                       ARRAY [
                           ROW (
                                ts.season,
                                ts.season_name,
                                ts.team,
                                ts.team_id,
                                ts.jersey_number,
                                ts.position,
                               ts.captain,
                               ts.yellow_cards,
                               ts.red_cards,
                               ts.yellow_red_cards,
                               ts.minutes_played,
                               ts.appearances,
                               ts.assists,
                               ts.lineups,
                               ts.goals,
                               ts.home_yellow_cards,
                               ts.away_yellow_cards,
                               ts.penalties,
                               ts.own_goals,
                               ts.goals_conceded
                               )::season_stats
                           ]
        )
    ELSE ls.season_stats
  END AS season_stats,
  ts.season IS NOT NULL AS is_active,
  CASE
    WHEN ts.season IS NOT NULL THEN 0
    ELSE years_since_last_active + 1
  END AS years_since_last_active,
  COALESCE(ts.season, ls.current_season + 1) AS current_season
FROM
  last_season ls
  FULL OUTER JOIN this_season ts ON ls.player_id = ts.player_id
)

select * from final