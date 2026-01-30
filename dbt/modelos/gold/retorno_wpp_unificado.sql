{{ incremental_config(
    model_name='retorno_wpp_unificado',
    zone='gold',
    materialized='incremental',
    incremental_strategy = 'append',
    partitioned_by="ARRAY['execution_year','execution_month']",
	on_schema_change='sync_all_columns'
) }}

WITH base AS (
    SELECT
        primkey,
        assetid,
        contactkey,
        mobilenumber,
        trackingtype,
        eventdateutc,
        status,
        enviado,
        entregue,
        lido,
        falhou,
        jbdefinitionid,
        jbactivityid,
        sendidentifier,
        activityname,
        execution_year,
        execution_month
    FROM  {{ ref('retorno_wpp_detalhado')}}  
    WHERE trackingtype IN ('SEND', 'DeliveryReport')
),

send_registros AS (
    SELECT
        jbdefinitionid,
        jbactivityid,
        sendidentifier,
        assetid,
        contactkey,
        MIN(eventdateutc) AS dt_primeiro_envio
    FROM base
    WHERE trackingtype = 'SEND'
    GROUP BY jbdefinitionid, jbactivityid, sendidentifier, assetid, contactkey
),
base_registros AS (
    SELECT
        jbdefinitionid,
        jbactivityid,
        sendidentifier,
        assetid,
        contactkey,
        MIN(eventdateutc) AS dt_primeiro_evento
    FROM base
    GROUP BY jbdefinitionid, jbactivityid, sendidentifier, assetid, contactkey
),
consolidado AS (
    SELECT
        jbdefinitionid,
        jbactivityid,
        sendidentifier,
        assetid,
        contactkey,
        MAX(activityname) AS nome_da_comunicacao,
        MAX(mobilenumber) AS phone,
        MAX(eventdateutc) AS dt_ultimo_status,

        MAX(enviado) AS fl_enviado,
        MAX(entregue) AS fl_entregue,
        MAX(lido) AS fl_lido,
        MAX(falhou) AS fl_falhou,

        CASE 
            WHEN MAX(falhou) = 1 THEN 'Falhou'
            WHEN MAX(lido) = 1 THEN 'Lido'
            WHEN MAX(entregue) = 1 THEN 'Entregue'
            WHEN MAX(enviado) = 1 THEN 'Enviado'
            ELSE 'Desconhecido'
        END AS status_final,

        MAX(execution_year) AS execution_year,
        MAX(execution_month) AS execution_month
    FROM base
    GROUP BY jbdefinitionid, jbactivityid, sendidentifier, assetid, contactkey
)

SELECT
    COALESCE(c.assetid, sr.assetid) AS assetid,
    COALESCE(c.sendidentifier, sr.sendidentifier) AS sendidentifier, 
    COALESCE(c.contactkey, sr.contactkey) AS contactkey,
    c.nome_da_comunicacao,
    CAST(
        COALESCE(sr.dt_primeiro_envio, br.dt_primeiro_evento) AS DATE
    ) AS dt_disparo,
    c.fl_enviado,
    c.fl_entregue,
    c.fl_lido,
    c.fl_falhou,
    c.status_final,
    'WhatsApp' AS plataforma,
    c.execution_year,
    c.execution_month
FROM consolidado c
LEFT JOIN send_registros sr
    ON c.assetid = sr.assetid
    AND c.contactkey = sr.contactkey
    AND c.jbdefinitionid = sr.jbdefinitionid
    AND c.jbactivityid = sr.jbactivityid
    AND c.sendidentifier = sr.sendidentifier
LEFT JOIN base_registros br
    ON c.assetid = br.assetid
    AND c.contactkey = br.contactkey
    AND c.jbdefinitionid = br.jbdefinitionid
    AND c.jbactivityid = br.jbactivityid
    AND c.sendidentifier = br.sendidentifier