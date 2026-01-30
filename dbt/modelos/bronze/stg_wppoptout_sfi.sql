{{ 
	standard_config(
		model_name='stg_wppoptout_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
	lower(TO_HEX(SHA256(TO_UTF8(concat(id_salesforce, datahora_sair, nome_do_disparo))))) as primkey,
	id_salesforce,
	phone,
	try_cast (datahora_sair AS timestamp)  as datahora_sair,
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
	{{ source('comunicacoes_bronze', 'wppoptout_sfi') }}