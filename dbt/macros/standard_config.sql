{% macro standard_config(model_name, zone, partitioned_by_column=none, incremental_strategy='append',materialized='table', on_table_exists='drop', on_schema_change='fail') %}

    {% if partitioned_by_column is none %}
        {{
        config(
            materialized=materialized,
            properties = {
                'format': "'PARQUET'"
            },
            on_schema_change=on_schema_change,
            incremental_strategy=incremental_strategy,
            on_table_exists=on_table_exists
        )
        }}
    {% else %}
        {{
        config(
            materialized=materialized,
            properties = {
                'format': "'PARQUET'",
                "partitioned_by": partitioned_by_column
            },
            on_schema_change=on_schema_change,
            pre_hook="set session hive.insert_existing_partitions_behavior='OVERWRITE'",
            on_table_exists=on_table_exists
        )
        }}
    {% endif %}
 
{% endmacro %}