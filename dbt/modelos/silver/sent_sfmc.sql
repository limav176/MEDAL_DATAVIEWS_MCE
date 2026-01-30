{{ incremental_config(
    model_name='sent_sfmc',
    zone='silver',
    materialized='incremental',
    unique_key='primkey',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']"
) }}

with max_exec as (
    select
        primkey,
        max(execution_date) as max_execution_date,
        max(ingestion_date) as max_ingestion_date
    from
         {{ ref('stg_sent_sfi')}} 
{% if is_incremental() %}
    WHERE execution_year = date_format(date_add('day', -1, current_date), '%Y%')
    and  eventdate between date_add('day', -7, current_date) and current_date
{% endif %}
    group by primkey
),
ranked_exec as (
    select
        s.*,
        row_number() over(partition by s.primkey order by s.execution_date desc, s.ingestion_date desc) as rn
    from
        {{ ref('stg_sent_sfi')}}  s 
    inner join max_exec
        ON s.primkey = max_exec.primkey
        AND s.execution_date = max_exec.max_execution_date
        AND s.ingestion_date = max_exec.max_ingestion_date
)
select
    primkey,
    substr(client_id, 1, 100) as client_id,
    substr(partnerkey, 1, 100) as partnerkey,
    substr(listid, 1, 100) as listid,
    substr(subscriberid, 1, 100) as subscriberid,
    substr(objectid, 1, 100) as objectid,
    substr(sendid, 1, 100) as sendid,
    subscriberkey,
    eventdate,
    substr(eventtype, 1, 100) as eventtype,
    triggerersenddefinitionobjectid,
    substr(batchid, 1, 100) as batchid,
    substr(triggeredsendcustomerkey, 1, 100) as triggeredsendcustomerkey,
    partnerproperties,
    ingestion_date,
    execution_date,
    DATE_FORMAT(date_add('day', 1, eventdate), '%Y') as execution_year,
    DATE_FORMAT(date_add('day', 1, eventdate),'%m') as execution_month,
    DATE_FORMAT(date_add('day', 1, eventdate), '%d') as execution_day
from ranked_exec
where rn = 1