{{ incremental_config(
    model_name='email_unificado',
    zone='gold',
    materialized='incremental',
    unique_key='id_comunicacao',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['ingestion_year','ingestion_month','ingestion_day']"
) }}

SELECT
	 id_comunicacao
	, id_pessoa
	, bu
	, nome_da_comunicacao
	, email
	, dt_disparo
	, fl_recebeu
	, fl_abertura
	, fl_clique
	, tipo
	, plataforma
	, subject
	, bouncecategory
	, execution_date
	, ingestion_year
	, ingestion_month
	, ingestion_day
FROM
	{{ ref('retorno_email_unificado')}}      
{% if is_incremental() %}
WHERE ingestion_year = date_format(date_add('day', -1, current_date), '%Y%')
  and date(dt_disparo)  between date_add('day', -7, current_date) and current_date
{% endif %}