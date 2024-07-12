do $$
begin
    for cur_season in 2001..2022 loop
INSERT INTO
  tm_dim_players
WITH
  last_season AS (
    SELECT
      *
    FROM
      tm_dim_players
    WHERE
      current_season = cur_season
  ),
  this_season AS (
    SELECT
      *
    FROM
      tm_dim_player_stats
    WHERE
      season_id = cur_season + 1
  )
SELECT
  COALESCE(ts.player_id, ls.player_id) AS player_id,
  COALESCE(ts.fullname, ls.fullname) AS fullname,
  COALESCE(ts.position, ls.position) AS position,
  COALESCE(ts.position_group, ls.position_group) AS position,
  COALESCE(ts.nationality, ls.nationality) AS nationality,
  COALESCE(ts.international_team, ls.international_team) AS international_team,
  COALESCE(ts.image_path, ls.image_path) AS image_path,
  CASE
    WHEN ts.season_id IS NULL THEN ls.season_stats
    WHEN ts.season_id IS NOT NULL AND ls.season_stats IS NULL
        THEN
        ARRAY[
            ROW (
                ts.season_id,
                ts.team_id_arr,
                ts.team_arr,
                ts.captain,
                ts.market_value,
                ts.market_value_currency,
                ts.jersey_number,
                ts.goals,
                ts.assists,
                ts.own_goals,
                ts.minutes_played,
                ts.appearances,
                ts.lineups,
                ts.matches_coming_off_the_bench,
                ts.matches_substituted_off,
                ts.yellow_cards,
                ts.second_yellow_cards,
                ts.red_cards
      )::season_statistics
     ]
    WHEN ts.season_id IS NOT NULL AND ls.season_stats IS NOT NULL
        THEN ARRAY_CAT(ls.season_stats,
                       ARRAY [
                           ROW (
                ts.season_id,
                ts.team_id_arr,
                ts.team_arr,
                ts.captain,
                ts.market_value,
                ts.market_value_currency,
                ts.jersey_number,
                ts.goals,
                ts.assists,
                ts.own_goals,
                ts.minutes_played,
                ts.appearances,
                ts.lineups,
                ts.matches_coming_off_the_bench,
                ts.matches_substituted_off,
                ts.yellow_cards,
                ts.second_yellow_cards,
                ts.red_cards
                               )::season_statistics
                           ]
        )
    ELSE ls.season_stats
  END AS season_stats,
  ts.season_id IS NOT NULL AS is_active,
  CASE
    WHEN ts.season_id IS NOT NULL THEN 0
    ELSE years_since_last_active + 1
  END AS years_since_last_active,
  COALESCE(ts.season_id, ls.current_season + 1) AS current_season
FROM
  last_season ls
  FULL OUTER JOIN this_season ts ON ls.player_id = ts.player_id;
    end loop;
end; $$