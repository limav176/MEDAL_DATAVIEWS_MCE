{{ incremental_config(
    model_name='retorno_push_unificado',
    zone='silver',
    materialized='incremental',
    unique_key='id_comunicacao',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month']",
	on_schema_change='sync_all_columns'
) }}

WITH push AS (
SELECT 
	id_push as id_comunicacao
	, CASE
        WHEN LOWER(messagename) LIKE '%fraude%'
           OR LOWER(messagename) LIKE '%senha%' THEN 'Fraude'
           
       WHEN LOWER(messagename) LIKE '%cobr%'
           OR LOWER(campaigns) LIKE '%cobr%'
           OR  LOWER(messagename) LIKE '%cob%'
           OR LOWER(messagename) LIKE '%reneg%'THEN 'Cobrança'
           
       WHEN lower(messagename) like '%autono%'
           OR messagename LIKE '131508_PMM%' THEN 'Autônomos'
           
       WHEN LOWER(messagename) LIKE '%aquis%'
           OR LOWER(messagename) LIKE '%mgm%'
           OR messagename LIKE '131302_AQUIS%'  THEN 'Aquisição'
           
       WHEN LOWER(messagename) LIKE '%cart%'
           OR LOWER(messagename) LIKE '%gami%'
           OR LOWER(messagename) LIKE '%churn%'
           OR messagename LIKE '131302_PMM%'  THEN 'Cartões'
           
       WHEN LOWER(messagename) LIKE '%brand%'
           OR messagename LIKE '130503_PMM%' THEN 'Branding'
           
       WHEN messagename LIKE '130812_PMM%' THEN 'CXM'
       
       WHEN LOWER(messagename) LIKE '%credito%' AND  LOWER(messagename) NOT LIKE '%conta%'
           OR LOWER(campaigns) LIKE '%credito%' AND  LOWER(campaigns) NOT LIKE '%conta%'
           OR LOWER(messagename) LIKE '%fgts%'
           OR messagename LIKE '131303_PMM%'   THEN 'Crédito'
           
       WHEN lower(messagename) like '%autono%'
           or lower(campaigns) like '%autono%' THEN 'Autônomos'
           
       WHEN LOWER(messagename) LIKE '%conta%'
           OR LOWER(messagename) LIKE '%cashback%'
           OR LOWER(campaigns) LIKE '%conta%'
           OR LOWER(messagename) LIKE '%loja%'
           OR LOWER(messagename) LIKE '%gift%'
           OR LOWER(campaigns) LIKE '%loja%'
           OR messagename LIKE '131501_PMM%' THEN 'Conta'
           
      WHEN  messagename LIKE '131423_PMM%' THEN 'Consórcios'
       
      WHEN LOWER(messagename) LIKE '%pmm%' THEN 'PMM'
      ELSE 'Outros'
   END AS BU
	,messagename AS nome_da_comunicacao	
	,cast(' ' as varchar)  as phone
	,id_pessoa
	,dt_envio as dt_disparo
	,CASE WHEN status = 'Success' THEN 1 ELSE 0 END  AS fl_recebeu
	,CASE WHEN (messageopened = 'yes' OR dt_abertura IS NOT NULL) THEN 1 ELSE 0 END AS fl_abertura
	, 'Salesforce' AS plataforma
	,execution_year
    ,execution_month
FROM  {{ ref('push_sfmc') }} 
where   execution_year = date_format(date_add('day', -1, current_date), '%Y%')
--- sempre usar date_trunc para garantir que o filtro pegue o mês inteiro
and date_trunc('month', dt_envio ) >=  date_add('month', -1, date_trunc('month', current_date))
    )
SELECT
	id_comunicacao
	, CAST(BU AS varchar(10)) AS bu
	, nome_da_comunicacao
	, CAST(phone AS varchar) AS phone
	, id_pessoa
	, dt_disparo
	, fl_recebeu
	, fl_abertura
	, plataforma
	,execution_year
 	,execution_month
FROM
	push