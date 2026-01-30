{{ standard_config(
	model_name='controle_comunicacoes_email',
	zone='gold',
	materialized='table'
) }}


with  nomes_disparos as (
    SELECT 
    emailname,
    date(eventdate) AS dt_disparo,
    DAY(eventdate) AS dia_disparo,
    MONTH(eventdate) AS mes_disparo,
    YEAR(eventdate) AS ano_disparo,
    regexp_extract(TRIM(emailname), '^(.*?)_', 1) AS centro_custo,
    regexp_extract(TRIM(emailname), '_([^_]+)_', 1) AS area_disparadora,
    count(*) AS total_enviado
    FROM 
     {{ ref('sent_sfmc') }} a
left join {{ref('send_sfmc')}} b on a.sendid = b.id
WHERE a.execution_year IN ('2025', '2026')
and date(a.eventdate) >= date_trunc('month', date_add('month', -1, current_date))
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
    emailname as nome_da_comunicacao,
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
