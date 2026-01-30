{{ incremental_config(
    model_name='analitico_consumo_bu',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['ingestion_year','ingestion_month', 'ingestion_day']",
	on_schema_change='sync_all_columns'
) }}


SELECT 
	date(data_disparo) AS dt_envio,
	bu,
	sub, 
	tipo, 
	SUM(CASE WHEN canal = 'email' THEN total_enviado ELSE 0 END) AS qtd_email, 
	SUM(CASE WHEN canal = 'sms' THEN total_enviado ELSE 0 END) AS qtd_sms,
	SUM(CASE WHEN canal = 'push' THEN total_enviado ELSE 0 END) AS qtd_push,
	DATE_FORMAT(date_add('day', 1, data_disparo), '%Y') as ingestion_year,
    DATE_FORMAT(date_add('day', 1, data_disparo),'%m') as ingestion_month,
	DATE_FORMAT(date_add('day', 1, data_disparo), '%d') as ingestion_day
FROM 
	{{ ref('analitico_disparos_bu') }} 
{% if is_incremental() %}
WHERE ingestion_year IN ('2025', '2026')
and date(data_disparo) >= date_trunc('month', date_add('month', -1, current_date))
{% endif %}
GROUP BY 
	1, 2, 3, 4, 8, 9, 10