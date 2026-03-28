{% macro result_from_points(points_col) %}
    case
        when {{ points_col }} = 3 then 'W'
        when {{ points_col }} = 1 then 'D'
        else 'L'
    end
{% endmacro %}
