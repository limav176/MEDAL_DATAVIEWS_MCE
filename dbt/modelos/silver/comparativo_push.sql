{{ standard_config(
	model_name='comparativo_push', 
	zone='silver',
	materialized='table'
) }}


WITH push_bronze AS (
    SELECT 
       date(datetimesend) AS dt_disparo, 
        count(DISTINCT lower(concat(messageid, deviceid, systemtoken, requestid))) AS tt_push_bronze
    FROM 
         {{ ref('stg_push_sfi') }} 
      ---  comunicacoes_bronze.push_sfi 
    WHERE 
        date(datetimesend) between date_add('month', -1, current_date) and current_date
    GROUP BY 
        1 
),
push_silver AS (
    SELECT 
        date(dt_envio) AS dt_disparo, 
        count(DISTINCT id_push) AS tt_push_silver
    FROM 
           {{ ref('push_sfmc') }} 
      --  comunicacoes_silver.push_sfmc
    WHERE 
       date(dt_envio) between date_add('month', -1, current_date) and current_date
    GROUP BY 
        1
)
SELECT 
    COALESCE(s.dt_disparo, b.dt_disparo) AS dt_disparo,
    COALESCE(b.tt_push_bronze, 0) AS total_bronze,
    COALESCE(s.tt_push_silver, 0) AS total_silver,
    COALESCE(s.tt_push_silver, 0) - COALESCE(b.tt_push_bronze, 0) AS diff_silver_bronze
FROM 
    push_silver s
FULL OUTER JOIN 
    push_bronze b ON s.dt_disparo = b.dt_disparo
ORDER BY 
    s.dt_disparo DESC