with team_stats as (
    select * from {{ ref('stg_team_stats') }}

),

teams as (
    select * from {{ ref('stg_teams') }}

),

vw as (
    select 
        team_stats_id as id
        , team_stats.season_name as season
        , team_stats.team_id   as team_id
        , teams.team           as team
        , team_stats.type           as type
        , value_all_count
        , value_all_percentage
        , value_home_count
        , value_home_percentage
        , value_away_count
        , value_away_percentage
        , value_0_15_count
        , value_0_15_percentage
        , value_15_30_count
        , value_15_30_percentage
        , value_30_45_count
        , value_30_45_percentage
        , value_45_60_count
        , value_45_60_percentage
        , value_60_75_count
        , value_60_75_percentage
        , value_75_90_count
        , value_75_90_percentage
        , value_all_average
        , value_all_first
        , value_home_average
        , value_home_first
        , value_away_average
        , value_away_first
        , value_count
        , value_average
        , value_player_id
        , value_player_name
        , value_coach
        , value_coach_average
        , value_home_overall_percentage
        , value_away_overall_percentage
        , value_over_0_5_matches_count
        , value_over_0_5_matches_percentage
        , value_over_0_5_team_count
        , value_over_0_5_team_percentage
        , value_over_1_5_matches_count
        , value_over_1_5_matches_percentage
        , value_over_1_5_team_count
        , value_over_1_5_team_percentage
        , value_over_2_5_matches_count
        , value_over_2_5_matches_percentage
        , value_over_2_5_team_count
        , value_over_2_5_team_percentage
        , value_over_3_5_matches_count
        , value_over_3_5_matches_percentage
        , value_over_3_5_team_count
        , value_over_3_5_team_percentage
        , value_over_4_5_matches_count
        , value_over_4_5_matches_percentage
        , value_over_4_5_team_count
        , value_over_4_5_team_percentage
        , value_over_5_5_matches_count
        , value_over_5_5_matches_percentage
        , value_over_5_5_team_count
        , value_over_5_5_team_percentage
    from team_stats
    join teams 
    on team_stats.team_id = teams.team_id
    ),

final as (
    select
        id,
        season,
        team_id,
        max(team) as team,
        max(value_all_count) filter (where type = 'Goals') as goals_all_count,
        max(value_all_average) filter (where type = 'Goals') as goals_all_average,
        max(value_home_average) filter (where type = 'Goals') as goals_home_average,
        max(value_away_average) filter (where type = 'Goals') as goals_away_average,
        max(value_count) filter (where type = 'Redcards') as redcards_count,
        max(value_count) filter (where type = 'Yellowcards') as yellowcards_count,
        max(value_count) filter (where type = 'Yellowred Cards') as yellow_red_cards_all_count,
        max(value_all_count) filter (where type = 'Cleansheets') as cleansheets_count,
        max(value_all_average) filter (where type = 'Cleansheets') as cleansheets_average,
        max(value_home_average) filter (where type = 'Cleansheets') as cleansheets_home_average,
        max(value_away_average) filter (where type = 'Cleansheets') as cleansheets_away_average,
        max(value_all_count) filter (where type = 'Both Teams To Score') as both_teams_to_score_count,
        max(value_75_90_count) filter (where type = 'Scoring Minutes') as scoring_minutes_75_90_count,
        max(value_75_90_percentage) filter (where type = 'Scoring Minutes') as scoring_minutes_75_90_percentage,
        max(value_all_count) filter (where type = 'Failed To Score') as failed_to_score_all_count,
        max(value_75_90_count) filter (where type = 'Conceded Scoring Minutes') as conceded_scoring_minutes_75_90_count,
        max(value_75_90_percentage) filter (where type = 'Conceded Scoring Minutes') as conceded_scoring_minutes_75_90_percentage,
        max(value_all_count) filter (where type = 'Goals Conceded') as goals_conceded_all_count,
        max(value_all_average) filter (where type = 'Goals Conceded') as goals_conceded_all_average,
        max(value_home_average) filter (where type = 'Goals Conceded') as goals_conceded_home_average,
        max(value_away_average) filter (where type = 'Goals Conceded') as goals_conceded_away_average,
        max(value_all_count) filter (where type = 'Team Lost') as team_lost_count,
        max(value_all_average) filter (where type = 'Team Lost') as team_lost_average,
        max(value_home_average) filter (where type = 'Team Lost') as team_lost_home_average,
        max(value_away_average) filter (where type = 'Team Lost') as team_lost_away_average,
        max(value_average) filter (where type = 'Ball Possession %') as ball_possession_average,
        max(value_over_0_5_matches_percentage) filter (where type = 'Number Of Goals') as num_of_goals_over_0_5_matches_percentage,
        max(value_over_1_5_matches_percentage) filter (where type = 'Number Of Goals') as num_of_goals_over_1_5_matches_percentage,
        max(value_over_2_5_matches_percentage) filter (where type = 'Number Of Goals') as num_of_goals_over_2_5_matches_percentage,
        max(value_over_3_5_matches_percentage) filter (where type = 'Number Of Goals') as num_of_goals_over_3_5_matches_percentage,
        max(value_over_4_5_matches_percentage) filter (where type = 'Number Of Goals') as num_of_goals_over_4_5_matches_percentage,
        max(value_over_5_5_matches_percentage) filter (where type = 'Number Of Goals') as num_of_goals_over_5_5_matches_percentage,
        max(value_over_0_5_team_count) filter (where type = 'Number Of Goals') as num_of_goals_over_0_5_team_count,
        max(value_over_1_5_team_count) filter (where type = 'Number Of Goals') as num_of_goals_over_1_5_team_count,
        max(value_over_2_5_team_count) filter (where type = 'Number Of Goals') as num_of_goals_over_2_5_team_count,
        max(value_over_3_5_team_count) filter (where type = 'Number Of Goals') as num_of_goals_over_3_5_team_count,
        max(value_over_4_5_team_count) filter (where type = 'Number Of Goals') as num_of_goals_over_4_5_team_count,
        max(value_over_5_5_team_count) filter (where type = 'Number Of Goals') as num_of_goals_over_5_5_team_count,
        max(value_coach) filter (where type = 'Redcards') as coach_redcards,
        max(value_coach) filter (where type = 'Yellowcards') as coach_yellowcards
    from vw
    group by
        id,
        season,
        team_id
)

    select * from final