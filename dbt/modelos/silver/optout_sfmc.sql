{{ 	
	standard_config(
	model_name='optout_sfmc',
    zone='silver',
	materialized='table'
) }}

with base as (
SELECT
	SubscriberType            
	,cast(cast(DateUnsubscribed as varchar) as timestamp) as DateUnsubscribed
	,status           
	,cast(cast(DateJoined as varchar) as timestamp) as DateJoined
	,subscriberkey          
	,ingestion_date      
	,ingestion_year      
	,ingestion_month     
	,ingestion_day       
	,execution_date      
	,execution_year      
	,execution_month     
	,execution_day
	,ROW_NUMBER() OVER (PARTITION BY DateUnsubscribed ORDER BY ingestion_date DESC,RANDOM()) AS rownumber
FROM
      {{ ref('stg_optout_sfi') }} 
)
SELECT
	SubscriberType            
	,DateUnsubscribed
	,status           
	,DateJoined
	,subscriberkey          
	,ingestion_date      
	,ingestion_year      
	,ingestion_month     
	,ingestion_day       
	,execution_date      
	,execution_year      
	,execution_month     
	,execution_day
	FROM 
	base
