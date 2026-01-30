{{ incremental_config(
	model_name='retorno_wppclique',
	zone='silver',
	materialized='incremental',
    unique_key='primkey',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']"
) }}

with rownumber as (
SELECT
	primkey,
	id_salesforce,
	phone,
	datahora_clique,
	campanha,
	nome_do_disparo,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day,
	ROW_NUMBER() OVER (PARTITION BY primkey ORDER BY ingestion_date DESC,RANDOM()) AS rownumber
FROM
    {{ ref('stg_wppclique_sfi') }}
	 {% if is_incremental() %}
    WHERE execution_year = date_format(date_add('day', -1, current_date), '%Y%')
    and  datahora_clique between date_add('day', -7, current_date) and current_date
	 {% endif %}
)
SELECT primkey,
	id_salesforce,
	phone,
	datahora_clique,
	campanha,
	nome_do_disparo,
	
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	date_add('day', 1, datahora_clique) as execution_date,
    DATE_FORMAT(date_add('day', 1, datahora_clique), '%Y') as execution_year,
    DATE_FORMAT(date_add('day', 1, datahora_clique),'%m') as execution_month,
    DATE_FORMAT(date_add('day', 1, datahora_clique), '%d') as execution_day
FROM rownumber
WHERE rownumber = 1