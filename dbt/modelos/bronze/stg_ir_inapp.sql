{{ 
	standard_config(
		model_name='stg_ir_inapp',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT 
    lower(concat(InAppSendName, Eventdate, InAppSendID, InAppButtonID)) as primkey,
    InAppSendName,
    cast(cast(Eventdate as varchar) as timestamp) Eventdate, 
    InAppButtonID,
    InAppSendID,
    InAppButtonLabel,
    InAppUniqueSends,
    InAppDeviceDisplays,
    InAppDeviceDismisses,
    InAppDeviceDeliveries,
    InAppDeviceButtonClicks,
    ingestion_date,
    ingestion_year,
    ingestion_month,
    ingestion_day,
    execution_date,
    execution_year,
    execution_month,
    execution_day
FROM 
    {{ source('comunicacoes_bronze', 'irinapp_sfi') }} 