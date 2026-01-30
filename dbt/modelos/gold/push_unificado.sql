{{ incremental_config(
    model_name='push_unificado',
    zone='gold',
    materialized='incremental',
    unique_key='id_comunicacao',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month']",
	on_schema_change='sync_all_columns'
) }}

SELECT  
	id_comunicacao
	,bu
	,nome_da_comunicacao
	,phone	
	,id_pessoa
	,dt_disparo
	,fl_recebeu	
	,fl_abertura	 
	,plataforma
	,execution_year
    ,execution_month
FROM
		{{ ref('retorno_push_unificado')}}      
{% if is_incremental() %}
where   execution_year = date_format(date_add('day', -1, current_date), '%Y%')
--- sempre usar date_trunc para garantir que o filtro pegue o mês inteiro
and date_trunc('month', dt_disparo ) >=  date_add('month', -1, date_trunc('month', current_date))
{% endif %}
