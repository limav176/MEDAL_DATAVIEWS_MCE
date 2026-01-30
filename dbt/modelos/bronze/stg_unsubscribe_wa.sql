{{ 
	standard_config(
		model_name='stg_unsubscribe_wa',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT
	to_hex(md5(to_utf8(concat(mid, eid, contactkey, mobilenumber, firstdateutc)))) as primkey,
	mid,
	eid,
	contactkey,
	mobilenumber,
	channeltype,
	channelid,
	channelname,
	DATE_PARSE(REGEXP_REPLACE(CAST(firstdateutc AS varchar), ' AM| PM', ''),'%m/%d/%Y %I:%i:%s') AS firstdateutc,
	DATE_PARSE(REGEXP_REPLACE(CAST(lastdateutc AS varchar), ' AM| PM', ''),'%m/%d/%Y %I:%i:%s') AS lastdateutc,
	potentialbouncecount,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM
    {{ source('comunicacoes_bronze', 'unsubscribe_wa_sfi') }}