{{ 
	standard_config(
		model_name='stg_wppquarentena_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
	lower(TO_HEX(SHA256(TO_UTF8(concat(id_salesforce, datahora_quarentena, nome_do_disparo))))) as primkey,
	id_salesforce,
	phone,
	try_cast (datahora_quarentena AS timestamp)  as datahora_quarentena,
	campanha,
	nome_do_disparo,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM
	{{ source('comunicacoes_bronze', 'wppquarentena_sfi') }}