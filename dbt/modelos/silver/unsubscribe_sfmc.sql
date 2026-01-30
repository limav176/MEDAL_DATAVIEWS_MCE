{{ incremental_config(
    model_name='unsubscribe_sfmc',
    zone='silver',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by = "ARRAY['ingestion_year', 'ingestion_month']"
) }}

 with base_unsubscribe as (
select
    id_unsubscribe,
    max(execution_date) as max_execution_date,
    max(ingestion_date) as max_ingestion_date
from  {{ ref('stg_unsubscribe_sfi') }} 
    group by id_unsubscribe
)
select distinct 
    accountid
    ,id_email
    ,c.id_unsubscribe
    ,oybaccountid
    ,jobid
    ,listid
    ,batchid
    ,subscriberid
    ,subscriberkey
    ,eventdate
    ,isunique
    ,domain
    ,ingestion_date
    ,DATE_FORMAT(date_add('day', 1, eventdate), '%d') as ingestion_day
    ,execution_date
    ,execution_year
    ,execution_month
    ,execution_day
    ,DATE_FORMAT(date_add('day', 1, eventdate), '%Y') as ingestion_year
    ,DATE_FORMAT(date_add('day', 1, eventdate),'%m') as ingestion_month
from  {{ ref('stg_unsubscribe_sfi') }}  c 
inner join base_unsubscribe d
	on c.id_unsubscribe = d.id_unsubscribe
	and  c.execution_date = d.max_execution_date
    and  c.ingestion_date = d.max_ingestion_date
{% if is_incremental() %}
   WHERE execution_year = date_format(date_add('day', -1, current_date), '%Y%')
   and  eventdate between date_add('day', -7, current_date) and current_date
{% endif %}