{{ standard_config(
	model_name='comparativo_camadas_email', 
	zone='gold',
	materialized='table'
) }}


WITH email_silver_bronze AS (
    SELECT 
       dt_disparo, 
       total_silver,
       total_bronze,
       diff_silver_bronze
    FROM 
     {{ ref('comparativo_email') }} 
     --  comunicacoes_silver.comparativo_email
),
email_gold AS (
    SELECT 
        date(dt_envio) AS dt_disparo, 
        count(DISTINCT id_email) AS total_gold
    FROM 
         {{ ref('retorno_email_detalhado') }} 
       --   comunicacoes_gold.retorno_email_detalhado
    WHERE 
        date(dt_envio)  between date_add('month', -1, current_date) and current_date
    GROUP BY 
        1
)
SELECT 
    COALESCE(s.dt_disparo, g.dt_disparo) AS dt_disparo,
    COALESCE(s.total_bronze, 0) AS total_bronze,
    COALESCE(s.total_silver, 0) AS total_silver,
    COALESCE(g.total_gold, 0) AS total_gold,
    diff_silver_bronze,
    COALESCE(g.total_gold, 0) - COALESCE(s.total_bronze, 0)AS diff_gold_bronze,
    COALESCE(g.total_gold, 0) - COALESCE(s.total_silver, 0) AS diff_gold_silver
FROM 
    email_silver_bronze s
FULL OUTER JOIN 
    email_gold g ON g.dt_disparo = s.dt_disparo
ORDER BY 
    s.dt_disparo DESC