{% snapshot snp_players %}
    {{
        config(
            target_schema='snapshots',
            unique_key='player_id',
            strategy='check',
            check_cols=['team_id']
        )
    }}

    select 
        player_id,
        firstname,
        lastname,
        position1,
        position2,
        strong_foot,
        jersey_number,
        dob,
        team_id
    from {{ source('box2box_raw', 'players') }}

{% endsnapshot %}