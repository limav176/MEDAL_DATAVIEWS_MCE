{{ standard_config(
	model_name='analitico_monitoracontato',
	zone='gold',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('monitoracontato_sfmc') }}
)
SELECT
	  DataRegistro
	  ,CAST(TotalRegistros as int)  as TotalRegistros
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