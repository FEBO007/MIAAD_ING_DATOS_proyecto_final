{% macro export_query(sql, out_path) %}
  {% set results = run_query(sql) %}
  {% if execute %}
    {% do results.to_csv(out_path) %}
    {% do log("Wrote results to " ~ out_path, info=True) %}
  {% endif %}
{% endmacro %}