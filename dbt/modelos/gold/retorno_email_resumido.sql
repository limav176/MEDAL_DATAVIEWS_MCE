{{ incremental_config(
    model_name='retorno_email_resumido',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['ingestion_year','ingestion_month','ingestion_day']",
	on_schema_change='sync_all_columns'
) }}

SELECT 
	nome_da_comunicacao,
	date(dt_disparo) dt_disparo,
	bu,
	plataforma,
	count(id_comunicacao) AS total_enviado,
	sum(fl_recebeu) AS total_recebimento,
	sum(fl_abertura) AS total_abertura,
	sum(fl_clique) AS total_clique,
	DATE_FORMAT(date_add('day', 1, dt_disparo), '%Y') as ingestion_year,
    DATE_FORMAT(date_add('day', 1, dt_disparo),'%m') as ingestion_month,
	DATE_FORMAT(date_add('day', 1, dt_disparo), '%d') as ingestion_day
FROM 
	{{ ref('retorno_email_unificado')}}
{% if is_incremental() %}
  WHERE ingestion_year = date_format( current_date, '%Y%')
  and dt_disparo between date_add('day', -7, current_date) and current_date
{% endif %}
GROUP BY 1,2,3,4,9,10,11