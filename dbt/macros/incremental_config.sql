{% macro incremental_config(model_name, zone, on_schema_change='fail', partitioned_by=none, incremental_strategy='append', materialized='table', unique_key=none, on_table_exists='drop') %}

    {% if partitioned_by is none %}
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
        {% if materialized == 'incremental' and unique_key is not none %}
            {{
            config(
                materialized=materialized,
                properties = {
                    'format': "'PARQUET'",
                    "partitioned_by": partitioned_by
                },
                on_schema_change=on_schema_change,
                incremental_strategy=incremental_strategy,
                pre_hook="set session hive.insert_existing_partitions_behavior='OVERWRITE'",
                on_table_exists=on_table_exists,
                unique_key=unique_key
            )
            }}
        {% else %}
            {{
            config(
            materialized=materialized,
            properties = {
                'format': "'PARQUET'",
                "partitioned_by": partitioned_by
            },
            on_schema_change=on_schema_change,
            pre_hook="set session hive.insert_existing_partitions_behavior='OVERWRITE'",
            on_table_exists=on_table_exists
            )
            }}
        {% endif %}
    {% endif %}
{% endmacro %}
