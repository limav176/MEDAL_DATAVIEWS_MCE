{{ 
	standard_config(
		model_name='stg_viz_inapp_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
	lower(TO_HEX(SHA256(TO_UTF8(concat(id_salesforce, datahora_visualizacao, nome_da_comunicacao))))) as primkey,
	id_salesforce,
	phone,
	email,
	try_cast (datahora_visualizacao AS timestamp)  as datahora_visualizacao,
	campanha,
	nome_da_comunicacao,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM
{{ source('comunicacoes_bronze', 'viz_inapp_sfi') }}
	