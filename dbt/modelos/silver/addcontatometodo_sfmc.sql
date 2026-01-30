{{ standard_config(
	model_name='addcontatometodo_sfmc',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('stg_addcontatometodo_sfi') }}
)
SELECT
	  addedby
	  ,totalContatos
	  ,dtRegistro
	  ,Metodo
	  ,ingestion_date
	  ,ingestion_year
	  ,ingestion_month
	  ,ingestion_day
	  ,execution_date
	  ,execution_year
	  ,execution_month
	  ,execution_day
FROM 
 base