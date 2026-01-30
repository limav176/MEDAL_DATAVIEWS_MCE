{{ standard_config(
	model_name='tipocontato_sfmc',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('stg_tipocontato_sfi') }}
)
SELECT
	  total
	  ,tipo
	  ,dtRegistro
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