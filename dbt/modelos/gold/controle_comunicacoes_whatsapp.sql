{{ standard_config(
	model_name='controle_comunicacoes_whatsapp',
	zone='gold',
	materialized='table'
) }}

with ranked_messages as  (
SELECT 
    eventdateutc,        
    activityname,                 
    conversationtype,                 
    contactkey         
    ,row_number() over (partition by contactkey, activityname, date(eventdateutc)
                        order by eventdateutc asc) as rn
FROM 
    {{ ref('whatsapp_sfmc') }} 
WHERE execution_year IN ('2025', '2026')
and date(eventdateutc) >= date_trunc('month', date_add('month', -1, current_date))
and conversationtype is not null
)
, nomes_disparos as (
SELECT 
    activityname,
    date(eventdateutc) AS dt_disparo,
    DAY(eventdateutc) AS dia_disparo,
    MONTH(eventdateutc) AS mes_disparo,
    YEAR(eventdateutc) AS ano_disparo,
    conversationtype AS categoria,
    regexp_extract(TRIM(activityname), '^(.*?)_', 1) AS centro_custo,
    regexp_extract(TRIM(activityname), '_([^_]+)_', 1) AS area_disparadora,
    count(*) AS total_enviado
FROM 
       ranked_messages
 where rn = 1
GROUP BY 
    1, 2, 3, 4, 5, 6
)
, centro_custo AS (
SELECT 
    * 
FROM 
    {{ ref('centros_custo_bu') }}
)
SELECT 
    UPPER(activityname) as nome_da_comunicacao,
    UPPER(activityname) AS atividade,
    categoria,
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
