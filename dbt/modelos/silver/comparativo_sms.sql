{{ standard_config(
	model_name='comparativo_sms',
	zone='silver',
	materialized='table'
) }}


WITH sms_bronze AS ( 
    SELECT 
       date(date_add('hour', 3, createdatetime)) AS dt_disparo, 
        count(DISTINCT mobilemessagetrackingid) AS tt_sms_bronze
    FROM 
       {{ ref('stg_sms_sfi') }}  
      --  comunicacoes_bronze.sms_sfi 
    WHERE 
        date(date_add('hour', 3, createdatetime)) between date_add('month', -1, current_date) and current_date
    GROUP BY 
        1
),
sms_silver AS (
    SELECT 
        date(date_add('hour', 3, createdatetime)) AS dt_disparo, 
        count(DISTINCT mobilemessagetrackingid) AS tt_sms_silver
    FROM 
        {{ ref('sms_sfmc') }} 
       --   comunicacoes_silver.sms_sfmc 
    WHERE 
       date(createdatetime) between date_add('month', -1, current_date) and current_date
    GROUP BY 
        1
)
SELECT 
    COALESCE( s.dt_disparo, b.dt_disparo) AS dt_disparo,
    COALESCE(b.tt_sms_bronze, 0) AS total_bronze,
    COALESCE(s.tt_sms_silver, 0) AS total_silver,
    COALESCE(s.tt_sms_silver, 0) - COALESCE(b.tt_sms_bronze, 0) AS diff_silver_bronze
FROM 
    sms_silver s 
FULL OUTER JOIN 
    sms_bronze b ON s.dt_disparo = b.dt_disparo
ORDER BY 
    s.dt_disparo DESC
