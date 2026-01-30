{{ 
	standard_config(
		model_name='inbound_whatsapp_sfmc',
		zone='silver',
		materialized='table'
	)
}}

with inbound as (
SELECT
	chatmessagingmologid,
	channelid,
	channelname,
	channeltype,
	mobilenumber,
	messagedata,
	datecreatedutc,
	messagetype,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day,
	ROW_NUMBER() OVER (PARTITION BY chatmessagingmologid ORDER BY ingestion_date DESC,RANDOM()) AS rownumber
FROM
    {{ ref('stg_inbound_wa_sfi') }}
WHERE
    datecreatedutc IS NOT NULL
)
SELECT
	chatmessagingmologid,
	channelid,
	channelname,
	channeltype,
	mobilenumber,
	messagedata,
	datecreatedutc,
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
	inbound
WHERE
	rownumber = 1