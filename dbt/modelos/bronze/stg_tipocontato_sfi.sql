{{ 
	standard_config(
		model_name='stg_tipocontato_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT 
	total,
	tipo,
	dtRegistro,	
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
	FROM 
	 {{ source('comunicacoes_bronze', 'tipocontato_sfi') }}