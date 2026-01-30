{{ incremental_config(
    model_name='not_sent_sfmc',
    zone='silver',
    materialized='incremental',
    unique_key='primkey',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']",
	on_schema_change='sync_all_columns'
) }}



SELECT  
    primkey,
    clientid,            
    emailaddress,      
    subscriberid,        
    triggeredsendexternalkey,
    reason,              
    sendid,              
    subscriberkey,       
    listid,              
    eventdate,           
    eventtype,           
    batchid,             
    ingestion_date,      
    ingestion_year,      
    ingestion_month,     
    ingestion_day,       
    execution_date,      
	DATE_FORMAT(eventdate, '%Y') as execution_year,
    DATE_FORMAT(eventdate, '%m') as execution_month,
    DATE_FORMAT(eventdate, '%d') as execution_day
FROM 
    {{ ref('stg_not_sent_sfi') }}