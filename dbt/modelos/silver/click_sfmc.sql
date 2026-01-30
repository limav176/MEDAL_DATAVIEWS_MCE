{{ standard_config(
	model_name='click_sfmc',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	accountid,
	id_email,
	id_click,
	oybaccountid,
	jobid,
	listid,
	batchid,
	subscriberid,
	subscriberkey,
	eventdate,
	"domain",
	"url",
	linkname,
	linkcontent,
	isunique,
	triggerersenddefinitionobjectid,
	triggeredsendcustomerkey,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day,
	ROW_NUMBER() OVER(PARTITION BY id_click order by execution_date DESC, RANDOM()) as rownumber
FROM 
 {{ ref('stg_click_sfi')}} 
)
SELECT
	accountid,
	id_email,
	id_click,
	oybaccountid,
	jobid,
	listid,
	batchid,
	subscriberid,
	subscriberkey,
	eventdate,
	"domain",
	url,
	linkname,
	linkcontent,
	isunique,
	triggerersenddefinitionobjectid,
	triggeredsendcustomerkey,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_date,
	execution_year,
	execution_month,
	execution_day
FROM
	base
WHERE rownumber = 1


