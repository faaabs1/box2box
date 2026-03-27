with lineup_agg as (
    select
        player_id,
        season_id,
        sum(minutes_played)   as minutes_total,
        sum(games_started)    as starts_total,
        sum(times_subbed_in)  as subs_in_total,
        sum(games_started) + sum(times_subbed_in) as apps_total
    from {{ ref('int_player_lineup') }}
    group by player_id, season_id
)

select
    p.player_id,
    p.team_id,
    la.season_id,
    la.minutes_total,
    la.starts_total,
    la.subs_in_total,
    la.apps_total,
    coalesce(g.goals_scored, 0) as goals_total

from {{ ref('stg_players') }} p
join lineup_agg la
    on p.player_id = la.player_id
left join {{ ref('int_player_stats') }} g
    on p.player_id = g.player_id and la.season_id = g.season_id