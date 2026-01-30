{{ standard_config(
	model_name='retorno_optout_sfmc',
	zone='gold',
	materialized='table'
) }}

SELECT
	SubscriberType            
	,DateUnsubscribed
	,status           
	,DateJoined
	,subscriberkey as id_pessoa       
	,ingestion_date      
	,ingestion_year      
	,ingestion_month     
	,ingestion_day       
	,execution_date      
	,execution_year      
	,execution_month     
	,execution_day
FROM
	{{ ref('optout_sfmc') }}