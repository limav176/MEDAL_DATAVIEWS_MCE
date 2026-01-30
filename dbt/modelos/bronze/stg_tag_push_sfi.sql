{{ 
	standard_config(
		model_name='stg_tag_push_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}
with source as (
select 
	* 
from 
	{{ source('comunicacoes_bronze', 'tag_push_sfi') }}
)
select
	deviceid            
	,apid                
	,value               
	,cast(cast(createddate as varchar) as timestamp) as createddate
	,createdby           
	,cast(cast(modifieddate as varchar) as timestamp) as modifieddate
	,modifiedby          
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
