{{ standard_config(
	model_name='analitico_tipocontato',
	zone='gold',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM 
 {{ ref('tipocontato_sfmc') }}
)
SELECT
	  CAST(total as int)  as TotalRegistros
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