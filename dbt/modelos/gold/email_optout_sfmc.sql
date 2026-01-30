{{ incremental_config(
    model_name='email_optout_sfmc',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['ingestion_year','ingestion_month', 'ingestion_day']",
	on_schema_change='sync_all_columns'
) }}

WITH dados_clientes AS (
		SELECT
		cd_cpf,
		id_cliente AS id_customer,
    ds_email,
		ds_origem  AS ds_origin,
		nm_social AS name,
		id_proposta_will AS id_proposal,
		lower(to_hex(sha256(to_utf8(lpad(replace(replace(cd_cpf, '.'), '-'), 11, '0'))))) AS id_pessoa
    FROM
    {{ source('cliente_silver', 'clientes') }} 
    where ds_origem = 'will'
) 
,base_optout2 as (
	SELECT 
		ds_origin,
		id_email,
		subscriberkey as id_pessoa,
		id_customer,
		id_proposal,
		"name",
		eventdate AS dt_unsubscribe,
		'Unsubscribe' AS eventtype
	FROM  {{ ref('retorno_unsubscribe_detalhado') }} u
	LEFT JOIN dados_clientes pessoa
		ON pessoa.id_pessoa = u.subscriberkey
)
SELECT 
	bo2.ds_origin,
	bo2.id_email,
	bo2.id_pessoa,
	bo2.id_proposal,
	bo2.id_customer,
	bo2.name,
	bo2.eventtype,
	bo2.dt_unsubscribe,
	r.dt_envio,
	r.email,
	r.activityname,
	r.emailname,
	r.journeyname,
	ingestion_year,
	ingestion_month,
	ingestion_day
FROM base_optout2 bo2
INNER JOIN {{ ref('retorno_email_detalhado') }} r
ON r.id_email = bo2.id_email
{% if is_incremental() %}
WHERE ingestion_year = date_format(date_add('day', -1, current_date), '%Y%')
AND date(dt_envio)  between date_add('day', -7, current_date) and current_date
{% else %}
WHERE ingestion_year in ('2024', '2023')
AND dt_envio >= date('2023-04-25')
{% endif %}