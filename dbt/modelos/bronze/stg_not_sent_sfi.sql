{{ 
	standard_config(
		model_name='stg_not_sent_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT  
    lower(TO_HEX(SHA256(TO_UTF8(concat(sendid , subscriberkey , eventdate , reason))))) as primkey,
    clientid,            
    emailaddress,      
    subscriberid,        
    triggeredsendexternalkey,
    reason,              
    sendid,              
    subscriberkey,       
    listid,              
    cast(cast(eventdate as varchar) as timestamp) eventdate,           
    eventtype,           
    batchid,             
    ingestion_date,      
    ingestion_year,      
    ingestion_month,     
    ingestion_day,       
    execution_date,      
    execution_year,      
    execution_month,     
    execution_day  
FROM 
    {{ source('comunicacoes_bronze', 'not_sent_sfi') }}