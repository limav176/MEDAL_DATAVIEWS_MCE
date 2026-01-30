{{ 
	standard_config(
		model_name='stg_sent_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

select 
	CONCAT(subscriberkey, sendid, batchid) AS primkey,
	client_id,
	partnerkey,
	listid,
	subscriberid,
	objectid,
	sendid,
	subscriberkey,
	eventdate,
	eventtype,
	triggerersenddefinitionobjectid,
	batchid,
	triggeredsendcustomerkey,
	partnerproperties,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM 
	{{ source('comunicacoes_bronze', 'sent_sfi') }}
	
