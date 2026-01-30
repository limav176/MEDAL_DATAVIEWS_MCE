{{ 
	standard_config(
		model_name='stg_empeleg_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT 
	documentNumberSeventhDigit,
	idRegistro,	
	id_pessoa,
	dataRegistro,
	firstName,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
	FROM 
	 {{ source('comunicacoes_bronze', 'empeleg_sfi') }}