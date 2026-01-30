{{ standard_config(
	model_name='retorno_click_sfmc',
	zone='gold',
	materialized='table'
) }}

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
	execution_day
FROM
	{{ ref('click_sfmc') }}