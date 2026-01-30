{{ 
	standard_config(
		model_name='stg_report_inapp_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
    inappsendname,       
    cast(cast(senddate as varchar) as timestamp)  senddate,    
    cast(inappuniquesends as int) as inappuniquesends,    
    cast(inappdevicedisplays as int) as inappdevicedisplays, 
    cast(inappdevicedismisses as int) as inappdevicedismisses,
    cast(inappdevicedeliveries as int) as inappdevicedeliveries,
    cast(inappdevicebuttonclicks as int) as inappdevicebuttonclicks,
    dtregistro,          
    ingestion_date,      
    ingestion_year,      
    ingestion_month,     
    ingestion_day,       
    execution_date,      
    execution_year,      
    execution_month,     
    execution_day    
FROM    
    {{ source('comunicacoes_bronze', 'report_inapp_sfi') }}