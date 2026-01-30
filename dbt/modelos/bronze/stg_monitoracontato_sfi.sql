{{ 
	standard_config(
		model_name='stg_monitoracontato_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT 
	DataRegistro,	
	TotalRegistros,
	NomeDE,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
	FROM 
	 {{ source('comunicacoes_bronze', 'monitoracontato_sfi') }}