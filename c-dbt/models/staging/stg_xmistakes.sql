SELECT
    xm.id as xmistake_id,
    xm.goal_id,
    xm.category,
    xm.errors as error_list,
    g.game_id
FROM {{ source('box2box_raw', 'xmistakes') }} xm
JOIN {{ref('stg_goals')}} g
    ON xm.goal_id = g.goal_id