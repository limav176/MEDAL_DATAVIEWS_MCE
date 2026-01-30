{{
    standard_config(
        model_name='controle_de_consumo_safra',
        zone='gold',
        materialized='table'
)}}
WITH dados_combinados AS (
    SELECT
        eventdateutc,
        activityname,
        conversationtype,
        contactkey,
        'Whatsapp' AS canal
    FROM
        {{ ref('whatsapp_sfmc') }} 
    WHERE
        execution_year = DATE_FORMAT(DATE_ADD('day', -1, CURRENT_DATE), '%Y%')
        AND eventdateutc >= DATE('2025-01-01')
        AND conversationtype IS NOT NULL 
    UNION ALL
    SELECT
        a.eventdate AS eventdateutc, 
        b.emailname AS activityname,
        'email' AS conversationtype,
        a.subscriberkey AS contactkey,
        'Email' AS canal
    FROM
        {{ ref('sent_sfmc') }} a
    LEFT JOIN 
        {{ ref('send_sfmc') }}  b ON a.sendid = b.id
    WHERE
        a.execution_year = DATE_FORMAT(DATE_ADD('day', -1, CURRENT_DATE), '%Y%')
        AND a.eventdate >= DATE('2025-01-01')

    UNION ALL

    SELECT
        createdatetime AS eventdateutc, 
        sms_name AS activityname,
        'sms' AS conversationtype,      
        id_pessoa AS contactkey,        
        'SMS' AS canal                  
    FROM
        {{ ref('sms_sfmc') }} 
    WHERE
        execution_year = DATE_FORMAT(DATE_ADD('day', -1, CURRENT_DATE), '%Y%') 
        AND createdatetime >= DATE('2025-01-01') 

    UNION ALL


    SELECT
        dt_envio AS eventdateutc,      
        messagename AS activityname,   
        'push' AS conversationtype,    
        id_pessoa AS contactkey,       
        'Push' AS canal                
    FROM
        {{ ref('push_sfmc') }}
    WHERE
        execution_year = DATE_FORMAT(DATE_ADD('day', -1, CURRENT_DATE), '%Y%') 
        AND dt_envio >= DATE('2025-01-01') 

    UNION ALL


    SELECT
        dt_envio AS eventdateutc,      
        messagename AS activityname,   
        'inapp' AS conversationtype,   
        id_pessoa AS contactkey,       
        'In-App' AS canal              
    FROM
        {{ ref('inapp_sfmc') }}
    WHERE
        execution_year = DATE_FORMAT(DATE_ADD('day', -1, CURRENT_DATE), '%Y%') 
        AND dt_envio >= DATE('2025-01-01') 
),
nomes_disparos AS (
    SELECT
        activityname,
        eventdateutc, 
        DATE(eventdateutc) AS dt_disparo,
        DAY(eventdateutc) AS dia_disparo,
        MONTH(eventdateutc) AS mes_disparo,
        YEAR(eventdateutc) AS ano_disparo,
        conversationtype AS categoria,
        REGEXP_EXTRACT(TRIM(activityname), '^(.*?)_', 1) AS centro_custo_raw,
        REGEXP_EXTRACT(TRIM(activityname), '_([^_]+)_', 1) AS area_disparadora,
        contactkey AS id_pessoa,
        canal
    FROM
        dados_combinados
),
centro_custo AS (
    SELECT
        *
    FROM
        {{ ref('centros_custo_bu') }}
)
SELECT 
    ep.id AS id_customer,
    COALESCE(b.bu, 'FORA DO PADRÃO') AS bu,
    COUNT(*) AS total_envios_no_mes_ano, 
    nd.canal,
    nd.ano_disparo,
    nd.mes_disparo    
FROM
    nomes_disparos nd
LEFT JOIN
    centro_custo b ON nd.centro_custo_raw = b.centro_custo AND nd.area_disparadora = b.disparado_por
LEFT JOIN
      {{ ref('envio_pessoa') }}  ep ON nd.id_pessoa = ep.id_pessoa 
GROUP BY
    ep.id,
    COALESCE(b.bu, 'FORA DO PADRÃO'),
    nd.canal,
    nd.ano_disparo,
    nd.mes_disparo
ORDER BY
    ep.id,
    nd.ano_disparo,
    nd.mes_disparo
