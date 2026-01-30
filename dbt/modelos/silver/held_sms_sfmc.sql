{{ incremental_config(
    model_name='held_sms_sfmc',
    zone='silver',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month','execution_day']",
	on_schema_change='sync_all_columns'
) }}

with base as (
SELECT
	lower(concat(CAST(holddate AS varchar), id_pessoa)) as primkey,
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
	DATE_FORMAT(date_add('day', 1, holddate), '%Y') as execution_year,
    DATE_FORMAT(date_add('day', 1, holddate),'%m') as execution_month,
    DATE_FORMAT(date_add('day', 1, holddate), '%d') as execution_day
FROM  
    {{ ref('stg_held_sms')}}
{% if is_incremental() %}
   WHERE date_trunc('month', holddate) >= date_trunc('month', date_add('month', -1, current_date))
{% endif %}
)
, aux as (
SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY primkey order by ingestion_date DESC, RANDOM()) as rownumber
FROM base 
)
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
FROM aux
WHERE rownumber = 1