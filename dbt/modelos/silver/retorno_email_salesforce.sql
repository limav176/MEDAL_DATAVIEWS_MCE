{{ incremental_config(
    model_name='retorno_email_salesforce',
    zone='silver',
    materialized='incremental',
    unique_key='id_email',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['ingestion_year','ingestion_month', 'ingestion_day']"
) }}

WITH  base_disparos AS (
SELECT
        primkey AS id_email,
        batchid,
        e.client_id,
        eventdate AS dt_envio,
        eventtype,
        listid,
        e.objectid,
        e.partnerkey,
        sendid,
        se.emailname,
        se.emailsubject AS subject,
        se.fromaddress,
        activityname,
        journeyname,
        versionnumber,
        journeystatus,
        subscriberid,
        subscriberkey AS id_pessoa,
        e.triggeredsendcustomerkey,
        e.triggerersenddefinitionobjectid,
        e.partnerproperties,
        e.execution_date,
        e.execution_year,
        e.execution_month,
        e.execution_day
    FROM 
        {{ ref('sent_sfmc') }} e
    LEFT JOIN 
        {{ ref('send_sfmc') }} se ON se.id = e.sendid
    LEFT JOIN 
        {{ ref('journey_sfmc') }} f ON f.journeyactivityobjectid = e.triggerersenddefinitionobjectid
{% if is_incremental() %}
     WHERE e.execution_year = date_format(date_add('day', -1, current_date), '%Y%')
  	 and date(date_add('hour', 3, eventdate)) between date_add('day', -7, current_date) and current_date
{% endif %}
)
,open_b AS (
    SELECT
        subscriberkey,
        triggerersenddefinitionobjectid,
        batchid,
        jobid AS sendid,
        COUNT(eventdate) AS nr_open,
        MIN(eventdate) AS dt_firstopen,
        MAX(eventdate) AS dt_lastopen
    FROM
        {{ ref('open_sfmc') }}
    GROUP BY 1, 2, 3, 4
)
,click AS (
    SELECT
        subscriberkey,
        triggerersenddefinitionobjectid,
        batchid,
        jobid AS sendid,
        COUNT(eventdate) AS nr_click,
        MIN(eventdate) AS dt_firstclick,
        MAX(eventdate) AS dt_lastclick
    FROM
  		{{ ref('click_sfmc') }}
    GROUP BY 1, 2, 3, 4
)
,bounce AS (
    SELECT
        subscriberkey,
        triggerersenddefinitionobjectid,
        batchid,
        jobid AS sendid,
        COUNT(eventdate) AS nr_bounce,
        MAX(eventdate) AS dt_bounce,
        MAX(bouncecategory) AS bouncecategory,
        MAX(smtpcode) AS smtpcode,
        MAX(smtpbouncereason) AS smtpreason
    FROM
    	{{ ref('bounce_sfmc') }}
    GROUP BY 1, 2, 3, 4
)
,unsubscribe AS (
    SELECT
        subscriberkey,
        batchid,
        jobid AS sendid,
        COUNT(eventdate) AS nr_unsubscribe,
        MAX(eventdate) AS dt_unsubscribe
    FROM
 		{{ ref('unsubscribe_sfmc') }}
    GROUP BY 1, 2, 3
)
,base_final AS (
	SELECT
		base.id_email,
        base.id_pessoa,
		base.triggerersenddefinitionobjectid,
		base.batchid,
		base.activityname,
		base.journeyname, 
		base.versionnumber, 
		base.journeystatus,
		base.sendid,
		base.emailname,
		base.subject,
		base.eventtype,
		dt_envio,
		base.fromaddress,
		case when base.fromaddress = 'info@e.willbank.com.br' then 'Transacional'
			else 'Comercial'
			end as tipo,
		nr_open,
		dt_firstopen ,
		dt_lastopen ,
		nr_click,
		dt_firstclick ,
		dt_lastclick ,
		nr_bounce,
		dt_bounce + INTERVAL '3' hour  dt_bounce,
		bouncecategory,
		smtpcode,
		smtpreason,
		nr_unsubscribe,
		dt_unsubscribe,
		base.execution_day,
    	base.execution_year,
    	base.execution_month
	FROM base_disparos base
	LEFT JOIN open_b p ON p.subscriberkey = base.id_pessoa AND p.batchid = base.batchid AND p.sendid = base.sendid
	LEFT JOIN click c ON c.subscriberkey = base.id_pessoa AND c.batchid = base.batchid AND c.sendid = base.sendid
	LEFT JOIN bounce b ON b.subscriberkey = base.id_pessoa AND b.batchid = base.batchid AND b.sendid = base.sendid
	LEFT JOIN unsubscribe u ON u.subscriberkey = base.id_pessoa AND u.batchid = base.batchid AND u.sendid = base.sendid
)
,deduplicated_base AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id_email ORDER BY (SELECT NULL)) AS rn
    FROM base_final
)
SELECT
	id_email,
    id_pessoa,
	triggerersenddefinitionobjectid,
	batchid,
	activityname,
	journeyname,
	versionnumber,
	journeystatus,
	sendid,
	emailname,
	subject,
	eventtype,
	dt_envio,
	fromaddress,
	tipo,
	nr_open,
	dt_firstopen,
	dt_lastopen,
	nr_click,
	dt_firstclick,
	dt_lastclick,
	nr_bounce,
	dt_bounce,
	bouncecategory,
	smtpcode,
	smtpreason,
	nr_unsubscribe,
	dt_unsubscribe,
	current_date as ingestion_date,
    DATE_FORMAT(date_add('hour', 27, dt_envio), '%Y') as ingestion_year,
    DATE_FORMAT(date_add('hour', 27, dt_envio),'%m') as ingestion_month,
	DATE_FORMAT(date_add('hour', 27, dt_envio), '%d') as ingestion_day
from 
	deduplicated_base
WHERE rn = 1