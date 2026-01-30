{{ standard_config(
	model_name='elegiveis_emp',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('stg_empeleg_sfi') }}
)
SELECT
	  documentNumberSeventhDigit
	  ,idRegistro
	  ,id_pessoa
	  ,dataRegistro
	  ,firstName
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