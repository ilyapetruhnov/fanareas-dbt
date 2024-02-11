CREATE TYPE season_stats AS
(
    season           INTEGER,
    captain          INTEGER,
    yellow_cards     INTEGER,
    red_cards        INTEGER,
    yellow_red_cards INTEGER,
    minutes_played   INTEGER,
    appearances      INTEGER,
    assists          INTEGER,
    lineups          INTEGER,
    goals            INTEGER,
    home             INTEGER,
    away             INTEGER,
    penalties        INTEGER,
    own_goals        INTEGER,
    goals_conceded   INTEGER
);

CREATE TABLE dim_players(
    player_id INT,
    firstname TEXT,
    lastname TEXT,
    date_of_birth TEXT,
    continent TEXT,
    nationality  TEXT,
    team TEXT,
    position TEXT,
    height  INT,
    weight  INT,
    season_stats season_stats[],
    is_active BOOLEAN,
    years_since_last_active INTEGER,
    current_season INT
  );