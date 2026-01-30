{{ standard_config(
	model_name='monitoracontato_sfmc',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('stg_monitoracontato_sfi') }}
)
SELECT
	  DataRegistro
	  ,TotalRegistros
	  ,NomeDE
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