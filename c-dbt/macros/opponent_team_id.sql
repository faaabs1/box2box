{% macro opponent_team_id(team_col, home_team_col, away_team_col) %}
    case when {{ team_col }} = {{ home_team_col }} then {{ away_team_col }} else {{ home_team_col }} end
{% endmacro %}
