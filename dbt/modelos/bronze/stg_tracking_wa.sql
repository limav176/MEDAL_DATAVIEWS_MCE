{{ 
	standard_config(
		model_name='stg_tracking_wa',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
    to_hex(md5(to_utf8(concat(trackingtype, mid, eid, contactkey, eventdateutc)))) AS primkey,
	trackingtype,
	mid,
	eid,
	contactkey,
	cast(cast(eventdateutc as varchar) as timestamp) as eventdateutc,
	channeltype,
	appid,
	channelid,
	channelname,
	status,
	reason,
	jbdefinitionid,
	jbactivityid,
	sendidentifier,
	assetid,
	messagetypeid,
	activityname,
	mobilenumber,
	messagedata,
	sendtype,
	conversationtype,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM
{{ source('comunicacoes_bronze', 'tracking_wa_sfi') }}
