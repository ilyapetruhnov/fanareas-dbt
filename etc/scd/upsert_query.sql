INSERT INTO dim_players
(SELECT * FROM batch_dim_players)
ON CONFLICT (player_id, current_season)
    DO UPDATE SET season_stats = EXCLUDED.season_stats,
                  is_active = EXCLUDED.is_active,
                  years_since_last_active = EXCLUDED.years_since_last_active;