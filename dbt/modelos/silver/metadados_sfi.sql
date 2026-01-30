{{ 
	standard_config(
		model_name='metadados_sfi',
		zone='silver',
		materialized='table'	
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
	 {{ ref('stg_metadados_sfi') }}