{{ standard_config(
	model_name='analitico_ingestaometodo',
	zone='gold',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('addcontatometodo_sfmc') }}
)
SELECT
	  addedby
	  ,CAST(totalContatos as int)  as TotalRegistros
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