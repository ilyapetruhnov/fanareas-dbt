truncate dim_players;

with dim_player_stats as (
    select * from {{ ref('dim_player_stats') }}
),

do $$
begin
    for cur_season in 2004..2022 loop
INSERT INTO
  dim_players
WITH
  last_season AS (
    SELECT
      *
    FROM
      dim_players
    WHERE
      current_season = cur_season
  ),
  this_season AS (
    SELECT
      *
    FROM
      dim_player_stats
    WHERE
      season = cur_season + 1
  )
SELECT
  COALESCE(ts.player_id, ls.player_id) AS player_id,
  COALESCE(ts.firstname, ls.firstname) AS firstname,
  COALESCE(ts.lastname, ls.lastname) AS lastname,
  COALESCE(ts.date_of_birth, ls.date_of_birth) AS date_of_birth,
  COALESCE(ts.continent, ls.continent) AS continent,
  COALESCE(ts.nationality, ls.nationality) AS nationality,
  COALESCE(ts.team, ls.team) AS team,
  COALESCE(ts.jersey_number, ls.jersey_number) AS jersey_number,
  COALESCE(ts.position, ls.position) AS position,
  COALESCE(ts.height, ls.height) AS height,
  COALESCE(ts.weight, ls.weight) AS weight,
  CASE
    WHEN ts.season IS NULL THEN ls.season_stats
    WHEN ts.season IS NOT NULL AND ls.season_stats IS NULL
        THEN
        ARRAY[
            ROW (
        ts.season,
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
  FULL OUTER JOIN this_season ts ON ls.player_id = ts.player_id;
    end loop;
end; $$