SELECT
    season_id,
    season_name,
    valid_from,
    valid_to
FROM {{ source('box2box_raw', 'seasons') }}