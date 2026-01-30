{{ incremental_config(
    model_name='retorno_email_detalhado',
    zone='gold',
    materialized='incremental',
    unique_key='id_push',
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
SELECT
  ds_origin,
  id_email,
  triggerersenddefinitionobjectid,
  batchid,
  a.id_pessoa,
  id_customer,
  id_proposal,
  ds_email as email,
  name,
  activityname,
  journeyname, 
  versionnumber, 
  journeystatus,
  sendid,
  emailname,
  subject,
  eventtype,
  dt_envio,
  fromaddress,
  tipo,
  nr_open,
  dt_firstopen,
  dt_lastopen,
  nr_click,
  dt_firstclick,
  dt_lastclick,
  nr_bounce,
  dt_bounce,
  bouncecategory,
  smtpcode,
  smtpreason,
  nr_unsubscribe,
  dt_unsubscribe,
  ingestion_year,
  ingestion_month,
  ingestion_day
FROM 
  {{ ref('retorno_email_salesforce') }} a
LEFT JOIN 
	dados_clientes b ON a.id_pessoa = b.id_pessoa
{% if is_incremental() %}
WHERE ingestion_year = date_format(date_add('day', -1, current_date), '%Y%')
  and date(dt_envio)  between date_add('day', -32, current_date) and current_date
{% endif %}