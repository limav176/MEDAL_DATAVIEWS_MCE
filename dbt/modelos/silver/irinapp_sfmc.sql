{{ incremental_config(
    model_name='irinapp_sfmc',
    zone='silver',
    materialized='incremental',
    unique_key='primkey',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']",
    on_schema_change='sync_all_columns'
) }}

with ranked_exec as (
    select  primkey,
    InAppSendName,
    Eventdate,
    InAppButtonID,
    InAppSendID,
    InAppButtonLabel,
    InAppUniqueSends,
    InAppDeviceDisplays,
    InAppDeviceDismisses,
    InAppDeviceDeliveries,
    InAppDeviceButtonClicks,
    execution_date,
        row_number() over(partition by primkey order by execution_date desc) as rn
    from
             {{ ref('stg_ir_inapp') }}
    WHERE InAppButtonID is not null 
    and ingestion_year = date_format(date_add('day', -1, current_date), '%Y')
    and date(Eventdate) between date_add('day', -7, current_date) and current_date
)

select 
    primkey as id_campaign,
    InAppSendName,
    Eventdate,
    InAppButtonID,
    InAppSendID,
    InAppButtonLabel,
    InAppUniqueSends as envios_unicos,
    InAppDeviceDisplays as exibicoes,
    InAppDeviceDismisses as dispensas,
    InAppDeviceDeliveries as entregas,
    InAppDeviceButtonClicks as cliques,
    DATE_FORMAT(date_add('hour', 24, Eventdate), '%Y') AS execution_year,
    DATE_FORMAT(date_add('hour', 24, Eventdate), '%m') AS execution_month,
    DATE_FORMAT(date_add('hour', 24, Eventdate), '%d') AS execution_day
from ranked_exec
where rn = 1