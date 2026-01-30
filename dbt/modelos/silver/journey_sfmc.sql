{{ standard_config(
	model_name='journey_sfmc',
	zone='silver',
	materialized='table'
) }}

WITH base AS (
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY activityid order by ingestion_date DESC, RANDOM()) as rownumber
FROM
 {{ ref('stg_journey_sfi') }}
)
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
 base
where rownumber = 1