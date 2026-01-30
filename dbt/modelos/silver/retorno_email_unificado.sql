{{ incremental_config(
    model_name='retorno_email_unificado',
    zone='silver',
    materialized='incremental',
    unique_key='id_comunicacao',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['ingestion_year','ingestion_month','ingestion_day']",
	on_schema_change='sync_all_columns'
) }}


with email as (
SELECT 
	id_email as id_comunicacao	
	, id_pessoa
,CASE
        WHEN LOWER(emailname) LIKE '%fraude%'
            OR LOWER(emailname) LIKE '%senha%' THEN 'Fraude'

        WHEN LOWER(emailname) LIKE '%cobr%'
            OR LOWER(journeyname) LIKE '%cobr%' 
            OR  LOWER(emailname) LIKE '%coba%'
            OR  LOWER(emailname) LIKE '%cob%'
            OR  LOWER(emailname) LIKE '%recuperacao%'
            OR  LOWER(emailname) LIKE '%feir%'
            OR LOWER(emailname) LIKE '%reneg%'THEN 'Cobrança'

        WHEN lower(emailname) like '%autono%' 
           OR emailname LIKE '131508_PMM%'  THEN 'Autônomos'

        WHEN LOWER(emailname) LIKE '%aquis%' 
            OR LOWER(emailname) LIKE '%mgm%'
           OR emailname LIKE '131302_AQUIS%'  THEN 'Aquisição'

        WHEN LOWER(emailname) LIKE '%cartoes%'
            OR LOWER(emailname) LIKE '%raspou%'
            OR LOWER(emailname) LIKE '%overlimit%'
            OR LOWER(emailname) LIKE '%gami%'
            OR LOWER(emailname) LIKE '%wallet%'
            OR emailname LIKE 'Logistica%' 
            OR emailname LIKE '11711_DIR_TEC_CART_%' 
            OR LOWER(emailname) LIKE '%_card_%'
           OR emailname LIKE '131302_PMM%' 
            OR LOWER(emailname) LIKE '%churn%' THEN 'Cartões'


        WHEN LOWER(emailname) LIKE '%brand%'
           OR emailname LIKE '130503_PMM%'  THEN 'Branding'

        WHEN emailname LIKE '130812_PMM%' THEN 'CXM'

        WHEN LOWER(emailname) LIKE '%credito%' AND  LOWER(emailname) NOT LIKE '%conta%'
            OR LOWER(journeyname) LIKE '%credito%' AND  LOWER(journeyname) NOT LIKE '%conta%'
            OR LOWER(emailname) LIKE '%cp_%' 
            OR LOWER(emailname) LIKE '%fgts%'
           OR emailname LIKE '131303_PMM%'  THEN 'Crédito'

        WHEN lower(emailname) like '%autono%' 
            OR lower(emailname) like '%willpay%' 
            OR lower(journeyname) like '%autono%' 
           OR emailname LIKE '131508_PMM%'  THEN 'Autônomos'

         WHEN LOWER(emailname) LIKE '%conta%'
         OR  LOWER(emailname) LIKE '%cashback%'
            OR LOWER(journeyname) LIKE '%conta%'
            OR LOWER(emailname) LIKE '%loja%' 
            OR LOWER(emailname) LIKE '%marketp%' 
            OR LOWER(emailname) LIKE '%gift%' 
            OR LOWER(emailname) LIKE '%cdb%' 
            OR LOWER(emailname) LIKE '%virtual_card%'
            OR LOWER(journeyname) LIKE '%loja%'
           OR emailname LIKE '131501_PMM%' THEN 'Conta'


        WHEN LOWER(emailname) LIKE '%cxm%' 
        OR LOWER(emailname) LIKE '%intercom%' THEN 'CXM'
        
      WHEN  emailname LIKE '131423_PMM%' THEN 'Consórcios'

        WHEN LOWER(emailname) LIKE '%pmm%' THEN 'PMM'
        ELSE 'Outros'
        END AS bu
	,emailname AS nome_da_comunicacao	
	,cast(' ' as varchar) AS email
	, dt_envio AS dt_disparo 
	, CASE WHEN  nr_bounce IS NULL THEN 1 ELSE 0 END  AS fl_recebeu 	
	,CASE WHEN nr_open IS NOT NULL THEN 1 ELSE 0 END AS  fl_abertura	
	,CASE WHEN nr_click  IS NOT NULL THEN 1 ELSE 0 END AS fl_clique	
	,tipo
	, 'Salesforce' AS plataforma
	,subject
	,bouncecategory
    ,current_date as execution_date
    ,DATE_FORMAT(date_add('day', 1, dt_envio), '%Y') as ingestion_year
    ,DATE_FORMAT(date_add('day', 1, dt_envio),'%m') as ingestion_month
	,DATE_FORMAT(date_add('day', 1, dt_envio), '%d') as ingestion_day
FROM 
     {{ ref('retorno_email_salesforce')}} 
WHERE date_trunc('month', dt_envio) >= date_trunc('month', date_add('month', -2, current_date))
AND  ingestion_year  = date_format(date_add('day', -1, current_date), '%Y%')
)
SELECT
	id_comunicacao
	, id_pessoa
	, CAST(bu AS varchar(10)) AS bu
	, nome_da_comunicacao
	, CAST(email AS varchar) AS email
	, dt_disparo
	, fl_recebeu
	, fl_abertura
	, fl_clique
	, tipo
	, plataforma
	, subject
	, bouncecategory
	, execution_date
	, ingestion_year
	, ingestion_month
	, ingestion_day
FROM
	email