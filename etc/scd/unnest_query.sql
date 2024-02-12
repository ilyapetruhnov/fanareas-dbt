SELECT
  firstname,
  lastname,
  array_to_string(team, '/') as team,
  t.*
FROM
  dim_players
  CROSS JOIN UNNEST (season_stats) AS t
WHERE
  current_season = 2023
  AND lastname = 'Arteta Amatriain';