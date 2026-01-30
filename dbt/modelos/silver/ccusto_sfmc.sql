{{ standard_config(
	model_name='ccusto_sfmc',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*
FROM
 {{ ref('stg_ccusto_sfi') }}
)
SELECT
	  nm_mensagem as nome_da_comunicacao
	  ,centro_custo
	  ,disparado_por
	  ,bu
	  ,bu_rateio
	  ,modifiedby
	  ,modifieddate
	  ,createddate
	  ,id
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