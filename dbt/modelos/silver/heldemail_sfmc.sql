{{ standard_config(
	model_name='heldemail_sfmc',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('stg_heldemail_sfi') }} 
)
SELECT
	  id_pessoa
	  ,email_status 
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