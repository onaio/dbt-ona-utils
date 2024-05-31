{% macro find_index(list, key, list2) %}
    {% for i in range(list | length) %}
        {% if list[i] == key %}
            {{return(list2[i])}}
        {% endif %}
    {% endfor %}
    {{return(key)}}
{% endmacro %}