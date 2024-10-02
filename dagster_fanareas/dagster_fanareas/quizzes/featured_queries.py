part_of_squad_query = """
with squad_view as (select tm_team.name as team_name, tm_squad.*
from tm_squad
join tm_team
on cast(tm_team.id as int) = tm_squad.team_id),
cs as (select player_id, name, team_name
            from squad_view
            where team_id = '{}'
              and season_id = {}
            order by market_value desc)
,
ls as (select player_id, name, team_name from squad_view
where team_id = '{}'
and season_id = {}
order by market_value desc),
ls_player as (
select * from ls
where player_id not in (select cs.player_id from cs)
limit 1
)
    (select player_id, name, team_name, false as current_player
     from squad_view
     where season_id = {}
     and player_id in (select player_id from ls_player)
     limit 1)
union
(select player_id, name, team_name, true as current_player
 from cs
 limit 3)
"""

top_player_stats_query = """
with vw as (select team_id_arr[1] as team_id, * from tm_dim_player_stats)
select * from vw
where team_id = '{}'
and season_id = {}
"""

position_played_query = """
with stats_view as (select team_id_arr[1] as team_id, * from tm_dim_player_stats)
,vw1 as (select tm_team.name as team_name, stats_view.*
from stats_view
join tm_team
on cast(tm_team.id as int) = stats_view.team_id)
,vw as (select *, ROW_NUMBER() OVER (PARTITION BY position ORDER BY RANDOM()) AS rn
            from vw1
            where team_id = '{}'
              and season_id = {}
            and position_group != 'goalkeeper'
            order by market_value desc
            )
SELECT player_id, fullname, team_name, position, position_group
FROM vw
WHERE rn = 1
limit 4
"""

standing_query = """
select season_id, rank, club_name, team_id, points, league_id 
from standing
where team_id = {}
and season_id = {}
"""

sent_off_query = """
(select fullname, team_arr[1] as team_name, league_name, position_group, second_yellow_cards, red_cards, true as sent_off  from tm_dim_player_stats
where '{}' = ANY(team_id_arr)
and season_id = {}
and (second_yellow_cards > 0 or red_cards > 0)
limit 1)
union(
select fullname, team_arr[1] as team_name, league_name, position_group, second_yellow_cards, red_cards, false as sent_off  from tm_dim_player_stats
where '{}' = ANY(team_id_arr)
and season_id = {}
and (second_yellow_cards = 0 and red_cards = 0)
limit 3)
"""

own_goals_query = """
(select fullname, team_arr[1] as team_name, league_name, position_group, own_goals, true as own_goal_scored from tm_dim_player_stats
where '{}' = ANY(team_id_arr)
and season_id = {}
and (own_goals > 0)
limit 1)
union(
select fullname, team_arr[1] as team_name, league_name, position_group, own_goals, false as own_goal_scored from tm_dim_player_stats
where '{}' = ANY(team_id_arr)
and season_id = {}
and (own_goals = 0 )
limit 3)
"""

biggest_win_query = """
with t as (select id, name from team)
,vw as (select *, row_number() over (partition by id order by id) as rn from matches)
,
    vw1 as (select *
                 , (cast(goals_home as int) - cast(goals_away as int)) as goal_difference
                 , CONCAT(goals_home, '-', goals_away)               AS final_score
            from vw
            where rn = 1
              and season_i_d = '{}'
              and winning_team = '{}'),
vw2 as (
    select vw1.*
    ,winner_team.name AS winner_team_name
    ,loser_team.name AS loser_team_name
   , row_number() over (order by goal_difference desc) as dif_rk from vw1
JOIN
t winner_team ON vw1.winning_team = winner_team.id
JOIN
t loser_team ON vw1.losing_team = loser_team.id
where goal_difference > 4
order by goal_difference desc
    )
select
    stadium_name,
    competition_name,
    winning_team,
    losing_team,
    winner_team_name,
    loser_team_name,
    final_score
from vw2
where dif_rk = 1
"""

team_options_query = """
select * from standing
where league_id = '{}'
and season_id = {}
and rank > 10
"""

biggest_loss_query = """
with t as (select id, name from team)
,vw as (select *, row_number() over (partition by id order by id) as rn from matches)
,
    vw1 as (select *
                 , (cast(goals_home as int) - cast(goals_away as int)) as goal_difference
                 , CONCAT(goals_home, '-', goals_away)               AS final_score
            from vw
            where rn = 1
              and season_i_d = '{}'
              and losing_team = '{}'),
vw2 as (
    select vw1.*
    ,winner_team.name AS winner_team_name
    ,loser_team.name AS loser_team_name
   , row_number() over (order by goal_difference desc) as dif_rk from vw1
JOIN
t winner_team ON vw1.winning_team = winner_team.id
JOIN
t loser_team ON vw1.losing_team = loser_team.id
where goal_difference > 5
order by goal_difference desc
    )
select
    stadium_name,
    competition_name,
    winning_team,
    losing_team,
    winner_team_name,
    loser_team_name,
    final_score
from vw2
where dif_rk = 1
"""

trophies_query = """
select
team_name,
array_agg(distinct trophy) as trophies
from tm_team_trophies
where team_id = '{}'
and season_ids = '{}'
group by team_name
"""

other_trophies_query = """
select
team_name,
array_agg(distinct trophy) || ARRAY['UEFA Super Cup', 'UEFA Champions League'] as trophies
from tm_team_trophies where team_id = '{}'
group by team_name
"""

transfers_query = """
with t as (select id, name from team),
     p as (select id, player_name from tm_player),
vw1 as (select *
        from transfer_records
        where is_loan is null
        and to_club_id = '{}'),
deduped as (select
    vw1.id as transfer_id
    ,vw1.season
    ,vw1.to_competition_id
    ,vw1.transfer_fee_value
    ,p.player_name
    ,from_team.name AS from_team_name
    ,to_team.name AS to_team_name
,row_number() over (partition by vw1.id order by vw1.id) as rn
FROM vw1
JOIN
t from_team ON vw1.from_team_id = from_team.id
JOIN
t to_team ON vw1.to_club_id = to_team.id
JOIN
p ON vw1.player_id = p.id)
select
    transfer_id,
    season,
    to_competition_id,
    player_name,
    from_team_name,
    to_team_name,
    transfer_fee_value
from deduped
where rn=1
"""

other_transfers_query = """
with t as (select id, name from team),
     p as (select id, player_name from tm_player),
final as (select
    transfer_records.id as transfer_id,
    season,
    player_name,
    from_team.name as from_team_name,
    to_team.name as to_team_name,
    transfer_fee_value,
    row_number() over (partition by transfer_records.id order by transfer_records.id) as rn
from transfer_records
JOIN
t from_team ON transfer_records.from_team_id = from_team.id
JOIN
t to_team ON transfer_records.to_club_id = to_team.id
JOIN
p ON transfer_records.player_id = p.id
where is_loan is null
and to_club_id != '{}'
and season = '{}'
and to_competition_id = '{}'
and "transferMarketValue_value" > 5000000)
select * from final where rn = 1
"""