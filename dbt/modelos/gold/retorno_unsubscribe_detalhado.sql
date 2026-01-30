{{ incremental_config(
    model_name='retorno_unsubscribe_detalhado',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']",
	on_schema_change='sync_all_columns'
) }}

SELECT DISTINCT
	subscriberkey 
	,accountid
	,id_email
	,id_unsubscribe
	,oybaccountid
	,jobid
	,listid
	,batchid
	,subscriberid
	,eventdate
	,isunique
	,"domain"
	,ingestion_date
	,ingestion_year
	,ingestion_month
	,ingestion_day
	,execution_date
	,execution_year
	,execution_month
	,execution_day
	from {{ ref('unsubscribe_sfmc')}} 
{% if is_incremental() %}
  WHERE date(eventdate) between date_add('day', -7, current_date) and current_date
{% endif %}

