{{ standard_config(
	model_name='retorno_journey_sfmc',
	zone='gold',
	materialized='table'
) }}

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
    {{ ref('journey_sfmc') }}