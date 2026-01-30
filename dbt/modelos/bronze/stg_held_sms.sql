{{ 
	standard_config(
		model_name='stg_held_sms',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
	id_customer,
	first_name,
	try_cast(created_at AS timestamp) AS created_at,
	mobilenumber,
	bouncecount,
	try_cast (holddate AS timestamp) AS holddate,
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
	{{ source('comunicacoes_bronze', 'sms_held_sfi') }}