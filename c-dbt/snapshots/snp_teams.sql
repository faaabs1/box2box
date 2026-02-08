{% snapshot snp_teams %}
    {{
        config(
            target_schema='snapshots',
            unique_key='team_id',
            strategy='check',
            check_cols=['league_id']
        )
    }}

    select 
        team_id,
        team_name,
        team_abb,
        league_id
    from {{ source('box2box_raw', 'teams') }}

{% endsnapshot %}