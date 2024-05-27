transfers_query = """with from_team as (
select name as fullname,
       player_id,
       team as from_team,
       from_team_id,
       to_team_id, date, completed, career_ended, amount from transfers
left join players
on transfers.player_id = players.id
left join stg_teams
on stg_teams.team_id = from_team_id
                                                         )
select
    fullname,
       player_id,
       from_team,
       stg_teams.team as to_team,
       from_team_id,
       to_team_id,
       date,
       completed,
       career_ended,
       amount
from from_team
join stg_teams
on stg_teams.team_id = to_team_id
where amount > 4000000
and completed = true
and fullname is not null
and from_team is not null
and stg_teams.team is not null
and date > '2016-01-01'
"""