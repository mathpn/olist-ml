WITH tb_join AS (
    SELECT DISTINCT
        t1.order_id,
        t1.customer_id,
        t2.seller_id,
        t3.*
    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    LEFT JOIN customers AS t3
    ON t1.customer_id = t3.customer_id

    WHERE purchase_timestamp < '{date}'
    AND purchase_timestamp >= date('{date}') - interval '6 months'
    AND seller_id IS NOT NULL
),
tb_group AS (
    SELECT
        seller_id,
        COUNT(DISTINCT desc_state) AS qty_state_order,
        COUNT(DISTINCT CASE WHEN desc_state = 'AC' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_AC,
        COUNT(DISTINCT CASE WHEN desc_state = 'AL' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_AL,
        COUNT(DISTINCT CASE WHEN desc_state = 'AM' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_AM,
        COUNT(DISTINCT CASE WHEN desc_state = 'AP' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_AP,
        COUNT(DISTINCT CASE WHEN desc_state = 'BA' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_BA,
        COUNT(DISTINCT CASE WHEN desc_state = 'CE' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_CE,
        COUNT(DISTINCT CASE WHEN desc_state = 'DF' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_DF,
        COUNT(DISTINCT CASE WHEN desc_state = 'ES' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_ES,
        COUNT(DISTINCT CASE WHEN desc_state = 'GO' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_GO,
        COUNT(DISTINCT CASE WHEN desc_state = 'MA' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_MA,
        COUNT(DISTINCT CASE WHEN desc_state = 'MG' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_MG,
        COUNT(DISTINCT CASE WHEN desc_state = 'MS' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_MS,
        COUNT(DISTINCT CASE WHEN desc_state = 'MT' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_MT,
        COUNT(DISTINCT CASE WHEN desc_state = 'PA' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_PA,
        COUNT(DISTINCT CASE WHEN desc_state = 'PB' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_PB,
        COUNT(DISTINCT CASE WHEN desc_state = 'PE' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_PE,
        COUNT(DISTINCT CASE WHEN desc_state = 'PI' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_PI,
        COUNT(DISTINCT CASE WHEN desc_state = 'PR' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_PR,
        COUNT(DISTINCT CASE WHEN desc_state = 'RJ' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_RJ,
        COUNT(DISTINCT CASE WHEN desc_state = 'RN' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_RN,
        COUNT(DISTINCT CASE WHEN desc_state = 'RO' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_RO,
        COUNT(DISTINCT CASE WHEN desc_state = 'RR' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_RR,
        COUNT(DISTINCT CASE WHEN desc_state = 'RS' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_RS,
        COUNT(DISTINCT CASE WHEN desc_state = 'SC' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_SC,
        COUNT(DISTINCT CASE WHEN desc_state = 'SE' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_SE,
        COUNT(DISTINCT CASE WHEN desc_state = 'SP' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_SP,
        COUNT(DISTINCT CASE WHEN desc_state = 'TO' THEN order_id END)::decimal / COUNT(DISTINCT order_id) AS pct_order_TO
    FROM tb_join GROUP BY seller_id
)
SELECT
    date('{date}') AS date_reference,
    NOW() AS date_ingestion,
    *
    FROM tb_group;