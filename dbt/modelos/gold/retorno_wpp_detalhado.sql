{{ incremental_config(
    model_name='retorno_wpp_detalhado',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']",
	on_schema_change='sync_all_columns'
) }}

SELECT
    primkey,
    trackingtype,
    mid,	
    eid,
    contactkey,
    eventdateutc,
    channeltype,
    appid,
    channelid,
    channelname,
    status, 
    CASE WHEN LOWER(Status) IN ('sent','delivered','read') THEN 1 ELSE 0 END AS Enviado,
    CASE WHEN LOWER(Status) IN ('delivered','read') THEN 1 ELSE 0 END AS Entregue,
    CASE WHEN LOWER(Status) = 'read' THEN 1 ELSE 0 END AS Lido,
    CASE WHEN LOWER(Status) IN ('failed','notsent') THEN 1 ELSE 0 END AS Falhou,
    reason,
    jbdefinitionid,
    jbactivityid,
    sendidentifier,
    assetid,
    messagetypeid,
    activityname,
    mobilenumber,
    messagedata,
    sendtype,
    conversationtype,
    ingestion_date,
    ingestion_year,
    ingestion_month,
    ingestion_day,
    execution_date,
    execution_year,
    execution_month,
    execution_day
FROM 
     {{ ref('whatsapp_sfmc') }}
{% if is_incremental() %}
WHERE date(eventdateutc) >= date_add('day', -7, current_date)
{% endif %}

