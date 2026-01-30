{{
    standard_config(
        model_name='controle_de_consumo_sfmc',
        zone='gold',
        materialized='table'
)}}

with consumo_email as (
SELECT
    nome_da_comunicacao,
    'email' as canal,
    'email' as categoria,
    centro_custo as codigo_bu,
    disparado_por,
    bu as area,
    bu_rateio as rateio,
    total_enviado,
    total_enviado as consumo,
    round(total_enviado * 0.00043,2) as custo_estimado,
    dt_disparo,
    dia_disparo,
    mes_disparo,
    ano_disparo
    from {{ ref('controle_comunicacoes_email') }}
)
, consumo_whatsapp as (
SELECT
    nome_da_comunicacao,
    'whatsapp' as canal,
    categoria,
    centro_custo as codigo_bu,
    disparado_por,
    bu as area,
    bu_rateio as rateio,
    total_enviado,
    CASE
        WHEN categoria = 'Marketing' THEN ROUND(total_enviado * 77.17, 0)
        WHEN categoria = 'Service'  THEN ROUND(total_enviado * 37.04, 0) 
        WHEN categoria = 'Authentication' THEN ROUND(total_enviado * 38.89, 0)
        WHEN categoria = 'Utility' THEN ROUND(total_enviado * 9.88, 0)
        ELSE 0 
    END AS consumo,
    CASE
        WHEN categoria = 'Marketing' THEN ROUND(total_enviado * 0.21197, 2)
        WHEN categoria = 'Service'  THEN ROUND(total_enviado * 0.023074, 2)
        WHEN categoria = 'Authentication'  THEN ROUND(total_enviado * 0.02307, 2)
        WHEN categoria = 'Utility'  THEN ROUND(total_enviado * 0.02307, 2)
        ELSE 0 
    END AS custo_estimado,
    dt_disparo,
    dia_disparo,
    mes_disparo,
    ano_disparo
FROM
     {{ ref('controle_comunicacoes_whatsapp') }}
)
, sms as (
SELECT
    nome_da_comunicacao,
    'sms' as canal,
    'sms/mms' as categoria,
    centro_custo as codigo_bu,
    disparado_por,
    bu as area,
    bu_rateio as rateio,
    total_enviado,
    round(total_enviado * 13.17,0) as consumo,
    round(total_enviado * 0.04759,2) as custo_estimado,
    dt_disparo,
    dia_disparo,
    mes_disparo,
    ano_disparo
from
    {{ ref('controle_comunicacoes_sms') }}
)
, inapp as (
    SELECT
        nome_da_comunicacao,
        'inapp' as canal,
        categoria,
        centro_custo as codigo_bu,
        disparado_por,
        bu as area,
        bu_rateio as rateio,
        total_enviado,
        total_enviado as consumo,
        round(total_enviado * 0.00043,2) as custo_estimado,
        dt_disparo,
        dia_disparo,
        mes_disparo,
        ano_disparo
    from {{ ref('controle_comunicacoes_inapp') }}
)
, push as (
    SELECT
        nome_da_comunicacao,
        'push' as canal,
        'push' as categoria,
        centro_custo as codigo_bu,
        disparado_por,
        bu as area,
        bu_rateio as rateio,
        total_enviado,
        total_enviado as consumo,
        round(total_enviado * 0.00043,2) as custo_estimado,
        dt_disparo,
        dia_disparo,
        mes_disparo,
        ano_disparo
    from {{ ref('controle_comunicacoes_push') }}
)
, uniao as (
    select * from consumo_email
    union all
    select * from consumo_whatsapp
    union all
    select * from sms
    union all
    select * from inapp
    union all
    select * from push
)
select * from uniao
