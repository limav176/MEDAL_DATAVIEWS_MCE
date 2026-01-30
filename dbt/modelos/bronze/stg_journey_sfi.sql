{{ 
	standard_config(
		model_name='stg_journey_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}

SELECT 
	versionid
	,activityid
	,activityname
	,activityexternalkey
	,journeyactivityobjectid
	,activitytype
	,journeyid
	,journeyname
	,versionnumber
	,createddate
	,lastpublisheddate
	,modifieddate
	,journeystatus
	,ingestion_date
	,ingestion_year
	,ingestion_month
	,ingestion_day
	,execution_date
	,execution_year
	,execution_month
	,execution_day
FROM 
	{{ source('comunicacoes_bronze', 'journey_sfi') }}

