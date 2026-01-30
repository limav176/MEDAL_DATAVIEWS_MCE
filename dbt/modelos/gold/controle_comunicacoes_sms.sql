{{ standard_config(
	model_name='controle_comunicacoes_sms',
	zone='gold',
	materialized='table'
) }}

with  nomes_disparos as (
    SELECT 
    sms_name,
    date(createdatetime) AS dt_disparo,
    DAY(createdatetime) AS dia_disparo,
    MONTH(createdatetime) AS mes_disparo,
    YEAR(createdatetime) AS ano_disparo,
    regexp_extract(TRIM(sms_name), '^(.*?)_', 1) AS centro_custo,
    regexp_extract(TRIM(sms_name), '_([^_]+)_', 1) AS area_disparadora,
    count(*) AS total_enviado
    FROM 
        {{ ref('sms_sfmc') }} 
WHERE execution_year IN ('2025', '2026')
and date(createdatetime) >= date_trunc('month', date_add('month', -1, current_date))
GROUP BY 
    1, 2, 3, 4, 5
)
, centro_custo AS (
SELECT 
    * 
FROM 
    {{ ref('centros_custo_bu') }}
)
SELECT 
    sms_name as nome_da_comunicacao,
    coalesce(b.centro_custo, 'FORA DO PADRÃO') centro_custo,
    coalesce(disparado_por ,'FORA DO PADRÃO') disparado_por, 
    coalesce(bu ,'FORA DO PADRÃO') bu,
    coalesce(bu_rateio ,'FORA DO PADRÃO') bu_rateio,
    total_enviado,
    dt_disparo,
    dia_disparo,
    mes_disparo,
    ano_disparo
from nomes_disparos a
left join centro_custo b  ON a.centro_custo = b.centro_custo and a.area_disparadora = b.disparado_por
order by ano_disparo desc, mes_disparo desc, dia_disparo desc
