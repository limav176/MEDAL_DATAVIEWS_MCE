{{ standard_config(
	model_name='envio_pessoa',
	zone='gold',
	materialized='table'
) }}

WITH leads_clean AS (
    SELECT 
        id,
        LOWER(TO_HEX(SHA256(TO_UTF8(LPAD(REPLACE(REPLACE(cpf, '.'), '-'), 11, '0'))))) AS id_pessoa,
        email AS ds_email,
        CASE
            WHEN length(trim(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''),'-',''))) = 10
            OR length(trim(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''),'-',''))) = 11
            OR length(trim(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''),'-',''))) = 12
            OR length(trim(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''),'-',''))) = 14
            THEN CONCAT('55', REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''),'-',''))
            ELSE REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''),'-','')
        END AS nr_celular,
        UPPER(COALESCE(name, legal_name)) AS nm_social,
        birth_date AS dt_nascimento,
        zip_code AS nr_cep,
        state_abbreviation AS ds_estado,
        UPPER(public_place) AS ds_logradouro,
        p.updated_date,
        cpf,
        ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY p.updated_date DESC) AS aux
    FROM   {{ source('onboarding_silver', 'propostas') }}  p
    JOIN   {{ source('onboarding_silver', 'endereco') }}  e ON e.proposal_id = p.id
    where p.updated_date >= date_add('day', -90, current_date) 
)
,leads_dedup AS (
    SELECT
        id,
        id_pessoa,
        ds_email,
        nr_celular,
        nm_social,
        dt_nascimento,
        nr_cep,
        ds_estado,
        ds_logradouro,
        cpf,
        'lead' as tipo,
        updated_date
    FROM leads_clean
    WHERE aux = 1
)
,clientes_clean AS (
    SELECT 
        c.id_cliente_will,
        LOWER(TO_HEX(SHA256(TO_UTF8(LPAD(REPLACE(REPLACE(cd_cpf, '.'), '-'), 11, '0'))))) AS id_pessoa, 
        ds_email,
        CASE
            WHEN length(trim(REPLACE(REPLACE(REPLACE(nr_celular_com_ddd, '(', ''), ')', ''),'-',''))) = 10
            OR length(trim(REPLACE(REPLACE(REPLACE(nr_celular_com_ddd, '(', ''), ')', ''),'-',''))) = 11
            OR length(trim(REPLACE(REPLACE(REPLACE(nr_celular_com_ddd, '(', ''), ')', ''),'-',''))) = 12
            OR length(trim(REPLACE(REPLACE(REPLACE(nr_celular_com_ddd, '(', ''), ')', ''),'-',''))) = 14
            THEN CONCAT('55', REPLACE(REPLACE(REPLACE(nr_celular_com_ddd, '(', ''), ')', ''),'-',''))
            ELSE REPLACE(REPLACE(REPLACE(nr_celular_com_ddd, '(', ''), ')', ''),'-','')
        END AS nr_celular,
        nm_social,
        dt_nascimento,
        st_conta,
        nr_cep,
        ds_estado,
        ds_logradouro,
        is_pep,
        dt_ultima_alteracao_will,
        cd_cpf,
        RANK() OVER (PARTITION BY cd_cpf ORDER BY dt_ultima_alteracao_will DESC, st_conta) AS aux
    FROM  {{ source('cliente_silver', 'clientes') }}  c
    JOIN  {{ source('wallet_gold', 'contas') }} ct ON c.cd_cpf = ct.cpf
    WHERE c.id_cliente_will IS NOT NULL
)
,clientes_dedup AS (
    SELECT
        id_cliente_will,
        id_pessoa,
        ds_email,
        nr_celular,
        nm_social,
        dt_nascimento,
        st_conta,
        nr_cep,
        ds_estado,
        ds_logradouro,
        is_pep,
        dt_ultima_alteracao_will,
        cd_cpf,
        'cliente' as tipo
    FROM clientes_clean
    WHERE aux = 1
)
,uniao AS (
    SELECT
        COALESCE(b.id_cliente_will, a.id) AS id,
        COALESCE(b.id_pessoa, a.id_pessoa) AS id_pessoa,
        COALESCE(b.ds_email, a.ds_email) AS ds_email,
        COALESCE(b.nr_celular, a.nr_celular) AS nr_celular,
        COALESCE(b.nm_social, a.nm_social) AS nm_social,
        COALESCE(b.dt_nascimento, a.dt_nascimento) AS dt_nascimento,
        b.st_conta,
        COALESCE(b.nr_cep, a.nr_cep) AS nr_cep,
        COALESCE(b.ds_estado, a.ds_estado) AS ds_estado,
        COALESCE(b.ds_logradouro, a.ds_logradouro) AS ds_logradouro,
        COALESCE(b.dt_ultima_alteracao_will, a.updated_date) AS updated_at,
        COALESCE(b.tipo, a.tipo) AS tipo,
        b.is_pep,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(b.id_pessoa, a.id_pessoa) ORDER BY b.dt_ultima_alteracao_will DESC) AS rank_row
    FROM leads_dedup a
    FULL OUTER JOIN clientes_dedup b ON a.id_pessoa = b.id_pessoa
)
SELECT
    id, 
    id_pessoa, 
    ds_email,
    nr_celular, 
    nm_social, 
    dt_nascimento,
    st_conta,
    nr_cep,
    ds_estado, 
    ds_logradouro, 
    is_pep,
    updated_at,
    'BR' as locale,
    tipo,
    current_date as ingestion_date,
    DATE_FORMAT(current_date, '%Y') as ingestion_year,
    DATE_FORMAT(current_date,'%m') as ingestion_month,
    DATE_FORMAT(current_date, '%d') as ingestion_day
FROM 
    uniao
WHERE rank_row = 1