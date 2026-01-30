{{ standard_config(
	model_name='comparativo_email', 
	zone='silver',
	materialized='table'
) }}


WITH email_bronze AS (
    SELECT 
       date(eventdate) AS dt_disparo, 
        count(DISTINCT CONCAT(subscriberkey, sendid, batchid)) AS tt_email_bronze
    FROM 
      {{ ref('stg_sent_sfi') }}   
      -- comunicacoes_bronze.sent_sfi
    WHERE 
        date(eventdate) between date_add('month', -1, current_date) and current_date
    GROUP BY 
        1
),
email_silver AS (
    SELECT 
        date(dt_envio) AS dt_disparo, 
        count(DISTINCT id_email) AS tt_email_silver
    FROM 
        {{ ref('retorno_email_salesforce') }}  
      --  comunicacoes_silver.retorno_email_salesforce
    WHERE 
       date(dt_envio)  between date_add('month', -1, current_date) and current_date
    GROUP BY 
        1
)
SELECT 
    COALESCE(s.dt_disparo, b.dt_disparo) AS dt_disparo,
    COALESCE(b.tt_email_bronze, 0) AS total_bronze,
    COALESCE(s.tt_email_silver, 0) AS total_silver,
    COALESCE(s.tt_email_silver, 0) - COALESCE(b.tt_email_bronze, 0) AS diff_silver_bronze
FROM 
    email_silver s 
FULL OUTER JOIN 
    email_bronze b ON s.dt_disparo = b.dt_disparo
ORDER BY 
    s.dt_disparo DESC

    