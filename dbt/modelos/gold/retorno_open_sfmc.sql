{{ standard_config(
	model_name='retorno_open_sfmc',
	zone='gold',
	materialized='table'
) }}


WITH part AS (
	SELECT
	id_open,
	max(execution_year) max_execution_year,
	max(execution_month) max_execution_month,
	max(execution_day) max_execution_day
FROM  {{ ref('open_sfmc') }}
GROUP BY 1
)
SELECT
	accountid,
	id_email,
	b.id_open,
	batchid,
	DOMAIN,
	eventdate,
	isunique,
	jobid,
	listid,
	oybaccountid,
	subscriberid,
	subscriberkey,
	triggeredsendcustomerkey,
	triggerersenddefinitionobjectid,
	ingestion_date,
	ingestion_year,
	ingestion_month,
	ingestion_day,
	execution_year,
	execution_month,
	execution_day
FROM part a
INNER JOIN {{ ref('open_sfmc') }} b
ON a.id_open = b.id_open 
AND execution_year = max_execution_year
AND execution_month = max_execution_month
AND execution_day =  max_execution_day
