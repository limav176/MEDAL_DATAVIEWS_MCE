{{ incremental_config(
    model_name='retorno_sms_resumido',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month']",
	on_schema_change='sync_all_columns'
) }}

SELECT 
	nome_da_comunicacao AS nome_comunicacao,
	dt_disparo AS data_disparo,
	bu,
	plataforma,
	count(DISTINCT id_comunicacao) AS total_enviado,
	sum(fl_recebeu) AS total_recebimento,
	execution_year,
	execution_month
FROM  
    {{ ref('retorno_sms_unificado')}}
{% if is_incremental() %}
WHERE execution_year = date_format(date_add('day', -1, current_date), '%Y%')
--- sempre usar date_trunc para garantir que o filtro pegue o mês inteiro
and date_trunc('month', dt_disparo ) >=  date_add('month', -1, date_trunc('month', current_date)) 
{% endif %}
GROUP BY 1,2,3,4,7,8