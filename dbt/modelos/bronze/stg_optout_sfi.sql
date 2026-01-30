{{ 
	standard_config(
		model_name='stg_optout_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}
with source as (
select 
	* 
from 
	{{ source('comunicacoes_bronze', 'optout_sfi') }}
)
select
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
from
	source
