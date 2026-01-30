{{ standard_config(
	model_name='monitoramento_falhas_email',
	zone='gold',
	materialized='table'
) }}
with email_sent as(
select 
    id_pessoa as subscriberkey,
    max(dt_envio) as  dt_last_sent,
    max(dt_lastopen) as dt_lastopen,
    max(dt_lastclick) as dt_lastclick
from {{ ref('retorno_email_detalhado') }} 
where 
ingestion_year >= date_format(date_add('year', -1, current_date), '%Y%')
group by 1
), 
bounce_data AS (
    SELECT 
        subscriberkey,
        COUNT(*) AS total_bounce,
        COUNT_IF(eventdate >= date_add('day', -30, current_date)) AS bounce_30d,
        MAX(eventdate) AS last_bounce_date,
        array_join(array_distinct(array_agg(bouncesubcategory)), ', ') AS bounce_reasons
    FROM {{ ref('bounce_sfmc') }}
    GROUP BY subscriberkey
)
,not_sent_data AS (
    SELECT 
        subscriberkey,
        COUNT(*) AS total_not_sent,
        COUNT_IF(eventdate >= date_add('day', -30, current_date)) AS not_sent_30d,
        MAX(eventdate) AS last_not_sent_date,
        array_join(array_distinct(array_agg(reason)), ', ') AS not_sent_reasons
    FROM {{ ref('not_sent_sfmc') }} 
    GROUP BY subscriberkey
),
optout_data AS (
    SELECT 
        subscriberkey,
        COUNT(*) AS total_optout,
        MAX(dateunsubscribed) AS last_optout_date
    FROM {{ ref('optout_sfmc') }} 
    GROUP BY subscriberkey
),
all_subscribers AS (
    SELECT subscriberkey FROM email_sent
    UNION
    SELECT subscriberkey FROM bounce_data
    UNION
    SELECT subscriberkey FROM not_sent_data
    UNION
    SELECT subscriberkey FROM optout_data
)
SELECT
    COALESCE(e.subscriberkey,b.subscriberkey,n.subscriberkey,o.subscriberkey) as subscriberkey,
    e.dt_last_sent as last_sent_date,
    e.dt_lastopen as last_open_date,
    e.dt_lastclick as last_click_date,
    COALESCE(b.total_bounce, 0) AS total_bounce,
    COALESCE(b.bounce_30d, 0) AS bounce_30d,
    DATE_DIFF('day', b.last_bounce_date, current_date) as days_since_last_bounce,
    bounce_reasons,
    COALESCE(n.total_not_sent, 0) AS total_not_sent,
    COALESCE(n.not_sent_30d, 0) AS not_sent_30d,
    DATE_DIFF('day', n.last_not_sent_date, current_date) as days_since_last_not_sent,
    n.not_sent_reasons,
    COALESCE(o.total_optout, 0) AS total_optout,
    DATE_DIFF('day', o.last_optout_date, current_date) as days_since_last_optout
FROM all_subscribers a
LEFT JOIN email_sent e ON a.subscriberkey = e.subscriberkey
LEFT JOIN bounce_data b ON a.subscriberkey = b.subscriberkey
LEFT JOIN not_sent_data n ON a.subscriberkey = n.subscriberkey
LEFT JOIN optout_data o ON a.subscriberkey = o.subscriberkey

