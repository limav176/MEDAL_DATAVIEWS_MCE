
{{ incremental_config(
    model_name='sms_unificado',
    zone='gold',
    materialized='incremental',
    unique_key='id_comunicacao',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month','execution_day']",
	on_schema_change='sync_all_columns'
) }}
SELECT 
	id_comunicacao	
	,bu	
	,nome_da_comunicacao	
	,dt_disparo
	,fl_recebeu	
	,plataforma
	,id_pessoa 
	,execution_date
	,execution_year
	,execution_month
	,execution_day
FROM
	{{ ref('retorno_sms_unificado')}}      
{% if is_incremental() %}
WHERE execution_year = date_format(date_add('day', -1, current_date), '%Y%')
  and date(dt_disparo)  between date_add('day', -7, current_date) and current_date
{% endif %}