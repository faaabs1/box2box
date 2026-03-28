{% macro points_from_goals(scored_col, conceded_col) %}
    case
        when {{ scored_col }} > {{ conceded_col }} then 3
        when {{ scored_col }} = {{ conceded_col }} then 1
        else 0
    end
{% endmacro %}
