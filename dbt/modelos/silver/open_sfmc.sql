{{ incremental_config(
    model_name='open_sfmc',
    zone='silver',
    materialized='incremental',
    unique_key='id_open',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']"
) }}

WITH base_open AS (
    SELECT
        accountid,
        id_email,
        id_open,
        batchid,
        "domain",
        eventdate,
        isunique,
        jobid,
        listid,
        oybaccountid,
        subscriberid,
        subscriberkey,
        triggeredsendcustomerkey,
        triggerersenddefinitionobjectid,
        ingestion_date,
        ingestion_year,
        ingestion_month,
        ingestion_day,
        DATE_FORMAT(date_add('day', 1, eventdate), '%Y') as execution_year,
        DATE_FORMAT(date_add('day', 1, eventdate),'%m') as execution_month,
        DATE_FORMAT(date_add('day', 1, eventdate), '%d') as execution_day
    FROM {{ ref('stg_open_sfi')}} 
)
, base2 AS (
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY id_open ORDER BY eventdate DESC, random()) as rownumber
FROM base_open
{% if is_incremental() %}
    WHERE execution_year = date_format(date_add('day', -1, current_date), '%Y%')
  	and date(eventdate) between date_add('day', -7, current_date) and current_date
{% endif %}
)
SELECT
    accountid,
    id_email,
    id_open,
    batchid,
    "domain",
    eventdate,
    isunique,
    jobid,
    listid,
    oybaccountid,
    subscriberid,
    subscriberkey,
    triggeredsendcustomerkey,
    triggerersenddefinitionobjectid,
    ingestion_date,
    ingestion_year,
    ingestion_month,
    ingestion_day,
    execution_year,
    execution_month,
    execution_day
FROM base2 
WHERE rownumber = 1
