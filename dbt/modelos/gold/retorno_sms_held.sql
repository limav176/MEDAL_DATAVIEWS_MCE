{{ incremental_config(
    model_name='retorno_sms_held',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month','execution_day']",
	on_schema_change='sync_all_columns'
) }}

SELECT
	primkey,
	id_customer,
	first_name,
	created_at,
	mobilenumber,
	bouncecount,
	holddate,
	id_pessoa,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM
    {{ ref('held_sms_sfmc')}}
{% if is_incremental() %}
   WHERE date_trunc('month', holddate) >= date_trunc('month', date_add('month', -1, current_date))
{% endif %}
