{{ 
	standard_config(
		model_name='report_inapp_sfmc',
		zone='silver',
		materialized='table'
	)
}}

SELECT
    inappsendname,       
    senddate,    
    inappuniquesends,    
    inappdevicedisplays, 
    inappdevicedismisses,
    inappdevicedeliveries,
    inappdevicebuttonclicks,
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
    {{ ref('stg_report_inapp_sfi') }}
