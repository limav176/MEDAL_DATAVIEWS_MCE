{{ incremental_config(
    model_name='retorno_push_detalhado',
    zone='gold',
    materialized='incremental',
    unique_key='id_push',
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
	id_push,
	a.id_pessoa,
	b.id_customer,
	id_proposal,
	"name",
	appname,
	messagename,
	tipo,
	messageid,
	template,
	format,
	deviceid,
	geofencename,
	pagename,
	campaigns,
	dt_envio,
	messagecontent,
	messageopened,
	dt_abertura,
	timeinapp,
	platform,
	platformversion,
	"status",
	pushjobid,
	systemtoken,
	inboxdownload,
	inboxopen,
	iosmediaurl,
	androidmediaurl,
	mediaalt,
	requestid,
	DATE_FORMAT(date_add('day', 1, dt_envio), '%Y') AS execution_year,
    DATE_FORMAT(date_add('day', 1, dt_envio), '%m') AS execution_month,
    DATE_FORMAT(date_add('day', 1, dt_envio), '%d') AS execution_day
FROM 
{{ ref('push_sfmc')}}  a
LEFT JOIN 
	dados_clientes b ON a.id_pessoa = b.id_pessoa
{% if is_incremental() %}
	WHERE execution_year = date_format(date_add('day', -1, current_date), '%Y%')
     and date(dt_envio)  between date_add('day', -7, current_date) and current_date
{% endif %}