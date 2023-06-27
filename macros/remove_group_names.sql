-- 2023.06.27 For fuad to turn into macro 

select 
    {% for column in colnames %}
    "{{column}}"::varchar  as
    {% if ('/' in column) and (column not in ['g11_43/g11_45/g11sub10_provinces', 'g11_43/g11_48/g11sub10_provinces']) -%}
             {{ column[column.rfind("/")+1:] }}
        {%- else -%} "{{column}}"
    {%- endif -%}

    {%- if not loop.last -%}
       , 
    {%- endif %}
    {% endfor %}