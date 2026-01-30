{{ 
	standard_config(
		model_name='stg_heldemail_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT 
	subscriberkey as id_pessoa, 
	status as email_status,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
	FROM 
	 {{ source('comunicacoes_bronze', 'heldemail_sfi') }}