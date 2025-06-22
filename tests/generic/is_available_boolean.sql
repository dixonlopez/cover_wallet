{% test is_available_boolean(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} not in (true, false) or {{ column_name }} is null
{% endtest %}