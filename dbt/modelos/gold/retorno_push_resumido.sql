{{ incremental_config(
    model_name='retorno_push_resumido',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month']",
	on_schema_change='sync_all_columns'
) }}

SELECT 
	nome_da_comunicacao as nome_comunicacao,
	date(dt_disparo) as data_disparo,
	bu,
	plataforma,
	count(id_comunicacao) AS total_enviado,
	sum(fl_recebeu) AS total_recebimento,
	sum(fl_abertura) AS total_abertura,
    execution_year,
    execution_month
FROM 
{{ ref('retorno_push_unificado')}}
{% if is_incremental() %}
where   execution_year = date_format(date_add('day', -1, current_date), '%Y%')
--- sempre usar date_trunc para garantir que o filtro pegue o mês inteiro
and date_trunc('month', dt_disparo ) >=  date_add('month', -1, date_trunc('month', current_date))
{% endif %}
GROUP BY 1,2,3,4,8,9