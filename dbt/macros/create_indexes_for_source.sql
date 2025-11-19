{% macro create_indexes_for_source(columns) -%}

    {% if execute %}
        {% set source_parsed = source(model.sources[0][0],model.sources[0][1])%}
        {% set source_table_name = source_parsed.identifier %}
        {{ log("Running index creation for source %s (table name being: %s) and columns %s" % (source_parsed,source_table_name,columns),info=True) }}
        {% set index_string = "CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (\"%s\");" %}
        {% for column in columns%}
            {% set column_parsed = column|replace(" ", "") %}
            {{ log(index_string % (source_table_name,column_parsed,source_parsed,column),info=True) }}
            {{ index_string % (source_table_name,column_parsed,source_parsed,column) }}
        {% endfor%}
    {% endif %}


{%- endmacro %}