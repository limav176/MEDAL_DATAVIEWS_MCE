{{ incremental_config(
    model_name='analitico_disparos_bu',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['ingestion_year','ingestion_month', 'ingestion_day']",
	on_schema_change='sync_all_columns'
) }}

WITH email AS (
    SELECT
    	'email' AS canal,
        activityname AS atividade,
        journeyname AS jornada,
        emailname AS comunicacao,
        tipo,
        date_trunc('day', dt_envio) AS dt_envio,
        count(id_email) AS total_enviado,
        ingestion_year,
        ingestion_month,
        ingestion_day
    FROM {{ ref('retorno_email_detalhado') }} 
    WHERE ingestion_year IN ('2025', '2026')
    AND date(dt_envio) >= date_trunc('month', date_add('month', -1, current_date))
    GROUP BY 1,2,3,4,5,6,8,9,10
), sms AS (
    SELECT
   		 'sms' AS canal,
        sms_name AS atividade,
        journeyname AS jornada,
        sms_name AS comunicacao,
        tipo,
        date_trunc('day', dt_envio) AS dt_envio,
        count(mobilemessagetrackingid) AS total_enviado,
        execution_year AS ingestion_year,
        execution_month AS ingestion_month,
        execution_day AS ingestion_day
    FROM  {{ ref('retorno_sms_detalhado') }} 
    WHERE execution_year IN ('2025', '2026')
    AND date(dt_envio) >= date_trunc('month', date_add('month', -1, current_date))
    GROUP BY 1,2,3,4,5,6,8,9,10
), push AS (
    SELECT
    	'push' AS canal,
        campaigns AS atividade,
        messagename AS jornada,
        messagename AS comunicacao,
        tipo,
        date_trunc('day', dt_envio) AS dt_envio,
        count(id_push) AS total_enviado,
        execution_year AS ingestion_year,
        execution_month AS  ingestion_month,
        execution_day AS ingestion_day
    FROM {{ ref('retorno_push_detalhado') }} 
    WHERE execution_year IN ('2025', '2026')
    AND date(dt_envio) >= date_trunc('month', date_add('month', -1, current_date))
    GROUP BY 1,2,3,4,5,6,8,9,10
)
, uniao AS (
SELECT * FROM email
UNION ALL
SELECT * FROM sms
UNION ALL
SELECT * FROM push
)
, fim AS (
SELECT 
	canal,
	atividade,
	jornada,
	comunicacao,
	tipo,
	dt_envio,
	total_enviado,
	CASE
        WHEN POSITION('fraude' IN LOWER(comunicacao)) > 0
            OR POSITION('senha' IN LOWER(comunicacao)) > 0 
            OR POSITION('fraude' IN LOWER(atividade)) > 0 THEN 'Fraude'
        WHEN POSITION('cobr' IN LOWER(comunicacao)) > 0
            OR POSITION('cob' IN LOWER(atividade)) > 0
            OR POSITION('cob' IN LOWER(comunicacao)) > 0
            OR POSITION('atraso' IN LOWER(comunicacao)) > 0
            OR POSITION('cobr' IN LOWER(jornada)) > 0
            OR POSITION('regu_' IN LOWER(jornada)) > 0
            OR POSITION('reneg' IN LOWER(comunicacao)) > 0 THEN 'Cobrança'
        WHEN POSITION('autono' IN LOWER(comunicacao)) > 0 THEN 'Autônomos'
        WHEN POSITION('aquis' IN LOWER(comunicacao)) > 0
            OR POSITION('mgm' IN LOWER(comunicacao)) > 0 THEN 'Aquisição'
        WHEN POSITION('cart' IN LOWER(comunicacao)) > 0
            OR POSITION('gami' IN LOWER(comunicacao)) > 0
            OR POSITION('logis' IN LOWER(atividade)) > 0 
            OR POSITION('logis' IN LOWER(jornada)) > 0 
            OR POSITION('cart' IN LOWER(atividade)) > 0
            OR POSITION('cart' IN LOWER(jornada)) > 0 
            OR POSITION('custo' IN LOWER(comunicacao)) > 0 
            OR POSITION('onboar' IN LOWER(comunicacao)) > 0 
            OR POSITION('aguardandoretirada' IN LOWER(comunicacao)) > 0 
            OR POSITION('logis' IN LOWER(comunicacao)) > 0
            OR POSITION('churn' IN LOWER(comunicacao)) > 0 THEN 'Cartões'
        WHEN POSITION('brand' IN LOWER(comunicacao)) > 0 THEN 'Branding'
        WHEN POSITION('cxm' IN LOWER(comunicacao)) > 0 THEN 'CXM'
         WHEN POSITION('intercom' IN LOWER(comunicacao)) > 0 THEN 'CXM'
        WHEN (POSITION('credito' IN LOWER(comunicacao)) > 0 AND POSITION('conta' IN LOWER(comunicacao)) = 0)
            OR (POSITION('credito' IN LOWER(atividade)) > 0 AND POSITION('conta' IN LOWER(atividade)) = 0)
            OR POSITION('cp' IN LOWER(comunicacao)) > 0
            OR POSITION('credito' IN LOWER(comunicacao)) > 0
            OR POSITION('credito' IN LOWER(jornada)) > 0
            OR POSITION('fgts' IN LOWER(comunicacao)) > 0 THEN 'Crédito'
        WHEN POSITION('conta' IN LOWER(comunicacao)) > 0
            OR POSITION('cashback' IN LOWER(comunicacao)) > 0
            OR POSITION('conta' IN LOWER(atividade)) > 0
            OR POSITION('conta' IN LOWER(comunicacao)) > 0
            OR POSITION('recharge' IN LOWER(atividade)) > 0
            OR POSITION('recharge' IN LOWER(jornada)) > 0
            OR POSITION('cdb' IN LOWER(atividade)) > 0
            OR POSITION('cdb' IN LOWER(comunicacao)) > 0
            OR POSITION('loja' IN LOWER(comunicacao)) > 0
            OR POSITION('market' IN LOWER(jornada)) > 0
            OR POSITION('gift' IN LOWER(comunicacao)) > 0
            OR POSITION('loja' IN LOWER(comunicacao)) > 0 THEN 'Conta'
        WHEN POSITION('pmm' IN LOWER(comunicacao)) > 0 THEN 'PMM'
        ELSE 'Outros'
    END AS bu,
	ingestion_year,
 	ingestion_month,
 	ingestion_day
FROM uniao
)
SELECT
	dt_envio AS data_disparo,
	canal,
	CAST(bu AS VARCHAR(10)) AS bu,
	CASE
		WHEN bu = 'Cartões' AND lower(comunicacao) LIKE '%gamif%' OR lower(comunicacao) LIKE '%fase%' THEN 'Gamificados'
		WHEN bu = 'Cartões' AND lower(comunicacao) LIKE '%log%' OR lower(comunicacao) LIKE '%entrega%' THEN 'Logística'
		WHEN bu = 'Cartões' AND lower(comunicacao) LIKE '%aqui%' THEN 'Aquisicao'
		WHEN bu = 'Cartões' AND lower(comunicacao) LIKE '%churn%' THEN 'Churn'
		WHEN bu = 'Crédito' AND lower(comunicacao) LIKE '%fgts%' OR lower(comunicacao) LIKE '%adesao%' THEN 'FGTS'
		WHEN bu = 'Crédito' AND lower(comunicacao) LIKE '%cp%' OR lower(comunicacao) LIKE '%emprestai%' THEN 'CP'
		WHEN bu = 'Fraude' AND lower(comunicacao) LIKE '%token%' THEN 'Token'
		WHEN bu = 'Conta'
		AND lower(comunicacao) LIKE '%loja%'
		OR lower(comunicacao) LIKE '%lj%'
		OR lower(comunicacao) LIKE '%market%' THEN 'Lojawill'
		WHEN bu = 'Cobrança'
		AND lower(comunicacao) LIKE '%cart%' THEN 'Cartoes'
		WHEN bu = 'Cobrança'
		AND lower(comunicacao) LIKE '%cred%'
		OR lower(comunicacao) LIKE '%cp%' THEN 'Crédito'
		WHEN bu = 'Cobrança'
		AND lower(comunicacao) LIKE '%feirao%' THEN 'Feirão'
		ELSE bu
		END AS sub,
	CAST (tipo AS VARCHAR(12))  AS tipo,
	COALESCE(comunicacao, atividade, jornada) as comunicacao,
	total_enviado,
	DATE_FORMAT(date_add('day', 1, dt_envio), '%Y') as ingestion_year,
    DATE_FORMAT(date_add('day', 1, dt_envio),'%m') as ingestion_month,
	DATE_FORMAT(date_add('day', 1, dt_envio), '%d') as ingestion_day
FROM
	fim
{% if is_incremental() %}
WHERE ingestion_year IN ('2025', '2026')
and date(dt_envio) >= date_trunc('month', date_add('month', -1, current_date))
{% endif %}