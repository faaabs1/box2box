{% macro game_location(team_col, home_team_col) %}
    case when {{ team_col }} = {{ home_team_col }} then 'home' else 'away' end
{% endmacro %}
