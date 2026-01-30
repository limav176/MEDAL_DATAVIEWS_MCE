{{ incremental_config(
    model_name='retorno_sms_detalhado',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']",
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
	s.id_pessoa,
	id_customer,
	pessoa.id_proposal,
	"name",
	s.mobilemessagetrackingid,
	s.sendid,
	s.messageid,
	case when lower(sms_name) like '%token%' or lower(sms_name) like '%fraude%' or lower(sms_name) like '%senha%' or lower(sms_name) like '%trans%' or lower(sms_name) like '%11711%' then 'Transacional'
		else 'Comercial'
		end as tipo,
	sms_name,
	s.mobile,
	s.shortcode,
	s.sent,
	s.description,
	s.delivered,
	date_add('hour', 3, s.createdatetime) as dt_envio,
	date_add('hour', 3, s.modifieddatetime) as dt_atualizacao,
	date_add('hour', 3, s.actiondatetime) as dt_recebimento_operadora,
	s.jbdefinitionid, 
	s.jbactivityid,
	j.journeyname, 
	j.versionnumber, 
	j.journeystatus,
	s.origin,
	s.istest,
	s.unsub,
	s.optin,
	s.optout,
	DATE_FORMAT(date_add('day', 1,s.createdatetime), '%Y') AS execution_year,
    DATE_FORMAT(date_add('day', 1,s.createdatetime), '%m') AS execution_month,
    DATE_FORMAT(date_add('day', 1,s.createdatetime), '%d') AS execution_day
FROM {{ ref('sms_sfmc') }} s
LEFT JOIN {{ ref('journey_sfmc') }} j
	on j.activityid = s.jbactivityid
	and j.versionid = s.jbdefinitionid
LEFT JOIN  dados_clientes pessoa
    on pessoa.id_pessoa = s.id_pessoa
{% if is_incremental() %}
WHERE s.execution_year = date_format(date_add('day', -1, current_date), '%Y%')
and date_add('hour', 3, s.createdatetime)  between date_add('day', -7, current_date) and current_date
{% endif %}