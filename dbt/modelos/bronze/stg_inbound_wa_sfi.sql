{{ 
	standard_config(
		model_name='stg_inbound_wa_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
	chatmessagingmologid,
	channelid,
	channelname,
	channeltype,
	mobilenumber,
	messagedata,
	try_cast(cast(datecreatedutc as varchar) as timestamp) as datecreatedutc,
	messagetype,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM
    {{ source('comunicacoes_bronze', 'inbound_wa_sfi') }}
