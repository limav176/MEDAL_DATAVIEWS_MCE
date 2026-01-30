{{ 
	standard_config(
		model_name='unsubscribe_whatsapp_sfmc',
		zone='silver',
		materialized='table'
	)
}}

with unsub as (
SELECT
	primkey,
	mid,
	eid,
	contactkey,
	mobilenumber,
	channeltype,
	channelid,
	channelname,
	CAST(firstdateutc AS TIMESTAMP) AS firstdateutc,
	CAST(lastdateutc AS TIMESTAMP) AS  lastdateutc,
	potentialbouncecount,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day,
	ROW_NUMBER() OVER (PARTITION BY primkey ORDER BY ingestion_date DESC,RANDOM()) AS rownumber
FROM
   {{ ref('stg_unsubscribe_wa') }}
)
SELECT
	primkey,
	mid,
	eid,
	contactkey,
	mobilenumber,
	channeltype,
	channelid,
	channelname,
	firstdateutc,
	lastdateutc,
	potentialbouncecount,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month
FROM
   unsub
where rownumber = 1