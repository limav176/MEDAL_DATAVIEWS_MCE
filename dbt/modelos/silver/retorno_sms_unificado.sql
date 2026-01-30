{{ incremental_config(
    model_name='retorno_sms_unificado',
    zone='silver',
    materialized='incremental',
    unique_key='id_comunicacao',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month']",
	on_schema_change='sync_all_columns'
) }}

WITH sms_geral AS (
SELECT 
	mobilemessagetrackingid id_comunicacao	
	,CASE
		 WHEN LOWER(sms_name) LIKE '%fraude%'
            OR LOWER(sms_name) LIKE '%senha%' THEN 'Fraude'
        WHEN LOWER(sms_name) LIKE '%cobr%'
            OR LOWER(sms_name) LIKE '%coba%' 
            OR  LOWER(sms_name) LIKE '%cob%'
            OR  LOWER(sms_name) in ('SMS_preventivo_vencimento')
        	OR LOWER(sms_name) LIKE '%reneg%'THEN 'Cobrança'
        WHEN lower(sms_name) like '%autono%'  THEN 'Autônomos'
        WHEN LOWER(sms_name) LIKE '%aquis%' 
        	OR LOWER(sms_name) LIKE '%mgm%' THEN 'Aquisição'
        WHEN LOWER(sms_name) LIKE '%cart%'
            OR LOWER(sms_name) LIKE '%gami%'
			OR LOWER(sms_name) LIKE '%logis%'
            OR LOWER(sms_name) LIKE '%churn%' THEN 'Cartões'
        WHEN LOWER(sms_name) LIKE '%brand%' THEN 'Branding'
        WHEN LOWER(sms_name) LIKE '%crm%' THEN 'CRM'
        WHEN LOWER(sms_name) LIKE '%credito%' AND  LOWER(sms_name) NOT LIKE '%conta%'
            OR LOWER(sms_name) LIKE '%credito%' AND  LOWER(sms_name) NOT LIKE '%conta%'
            OR LOWER(sms_name) LIKE '%fgts%' THEN 'Crédito'
        WHEN lower(sms_name) like '%autono%' 
            or lower(sms_name) like '%autono%' THEN 'Autônomos'
         WHEN LOWER(sms_name) LIKE '%conta%'
         OR  LOWER(sms_name) LIKE '%cashback%'
        	OR LOWER(sms_name) LIKE '%conta%'
        	OR LOWER(sms_name) LIKE '%loja%' 
        	OR LOWER(sms_name) LIKE '%gift%' 
        	OR LOWER(sms_name) LIKE '%loja%' THEN 'Conta'
       WHEN LOWER(sms_name) LIKE '%pmm%' THEN 'PMM'
        ELSE 'Outros'
	END AS BU
	,sms_name AS nome_da_comunicacao
	,actiondatetime AS dt_disparo
	,CASE WHEN delivered = 'True' THEN 1 ELSE 0 END  AS fl_recebeu	
	, 'Salesforce' AS plataforma
	, id_pessoa 
	,execution_date
	,ROW_NUMBER() OVER (PARTITION BY mobilemessagetrackingid ORDER BY execution_date DESC) AS rn 
FROM 
 {{ ref('sms_sfmc') }}  e
)
SELECT 
	id_comunicacao	
	,CAST(bu AS varchar(10)) AS bu
	,nome_da_comunicacao	
	,dt_disparo
	,fl_recebeu	
	,plataforma
	,id_pessoa 
	,execution_date
    ,DATE_FORMAT(dt_disparo, '%d') as execution_day
	,DATE_FORMAT(dt_disparo, '%Y') as execution_year
    ,DATE_FORMAT(dt_disparo, '%m') as execution_month
FROM
	sms_geral
WHERE  rn = 1
and  DATE_FORMAT(dt_disparo, '%Y')  = date_format(date_add('day', -1, current_date), '%Y%')
--- sempre usar date_trunc para garantir que o filtro pegue o mês inteiro
and date_trunc('month', dt_disparo ) >=  date_add('month', -1, date_trunc('month', current_date))