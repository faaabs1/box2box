select
    league_id,
    league_name,
    league_country
from {{ source('box2box_raw', 'leagues') }}