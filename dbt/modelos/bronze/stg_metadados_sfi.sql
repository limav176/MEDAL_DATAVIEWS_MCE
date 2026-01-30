{{ 
	standard_config(
		model_name='stg_metadados_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT 
	dag_name,
    volume,
    sendtime,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
	FROM 
	 {{ source('comunicacoes_bronze', 'metadados_sfi') }}