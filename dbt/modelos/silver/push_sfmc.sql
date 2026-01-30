{{ incremental_config(
    model_name='push_sfmc',
    zone='silver',
    materialized='incremental',
    unique_key='id_push',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month', 'execution_day']",
	on_schema_change='sync_all_columns'
) }}


with ranked_exec as (
    select
        primkey,
        contactkey,
        appname,
	    messagename,
	    messageid,
    	template,
    	format,
    	deviceid,
    	geofencename,
    	pagename,
    	campaigns,
    	datetimesend,
    	messagecontent,
    	messageopened,
    	opendate,
    	timeinapp,
    	plataform,
    	platformversion,
    	"status",
    	pushjobid,
    	systemtoken,
    	inboxmessagedownloaded,
    	inboxmessageopened,
    	iosmediaurl,
    	androidmediaurl,
    	mediaalt,
    	requestid,
    	execution_date,
        row_number() over(partition BY primkey order by execution_date desc) as rn
    from
           {{ ref('stg_push_sfi')}} 
    WHERE ingestion_year = date_format(date_add('day', -1, current_date), '%Y%')
  	and date(datetimesend)  between date_add('day', -7, current_date) and current_date
)
select 
	primkey as id_push,
	contactkey as id_pessoa,
	appname,
	messagename,
	CAST(
    (CASE 
        WHEN lower(messagename) LIKE '%trans%' OR lower(messagename) LIKE '%11711%' THEN 'Transacional'
        ELSE 'Comercial'
    END) AS VARCHAR) AS tipo,
	messageid,
	template,
	format,
	deviceid,
	geofencename,
	pagename,
	campaigns,
	datetimesend as dt_envio,
	messagecontent,
	messageopened,
	opendate as dt_abertura,
	timeinapp,
	plataform as platform,
	platformversion,
	"status",
	pushjobid,
	systemtoken,
	inboxmessagedownloaded as inboxdownload,
	inboxmessageopened as inboxopen,
	iosmediaurl,
	androidmediaurl,
	mediaalt,
	requestid,
	DATE_FORMAT(date_add('hour', 24, datetimesend), '%Y') AS execution_year,
    DATE_FORMAT(date_add('hour', 24, datetimesend), '%m') AS execution_month,
    DATE_FORMAT(date_add('hour', 24, datetimesend), '%d') AS execution_day
from ranked_exec
where rn = 1
