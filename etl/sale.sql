DROP TABLE IF EXISTS analytics.fs_sale;
CREATE TABLE analytics.fs_sale AS
WITH tb_join AS (
    SELECT
        t2.*,
        t1.purchase_timestamp
    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    WHERE purchase_timestamp < '2018-01-01'
    AND purchase_timestamp >= date('2018-01-01') - interval '6 months'
    AND seller_id IS NOT NULL
),
tb_summary AS (
    SELECT
        seller_id,
        COUNT(DISTINCT order_id) AS qty_orders,
        COUNT(DISTINCT date(purchase_timestamp)) AS qty_days,
        COUNT(product_id) AS qty_items,
        date_part(
            'day',
            '2018-01-01'::timestamp - max(purchase_timestamp::timestamp)
        ) AS qty_recency,
        sum(price) / COUNT(DISTINCT order_id) AS avg_ticket,
        avg(price) AS avg_price,
        max(price) AS max_price,
        min(price) AS min_price,
        COUNT(product_id) / COUNT(DISTINCT order_id) AS avg_no_products
    FROM tb_join
    GROUP BY seller_id
),
tb_order_summary AS (
    SELECT
        seller_id,
        order_id,
        SUM(price) AS order_price
    FROM tb_join
    GROUP BY 1, 2
),
tb_min_max AS (
    SELECT
        seller_id,
        min(order_price) AS min_order_price,
        max(order_price) AS max_order_price
    FROM tb_order_summary
    GROUP BY seller_id
),
tb_customer_life AS (
    SELECT
        t2.seller_id,
        sum(price) AS LTV,
        max(
            date_part(
                'day',
                '2018-01-01'::timestamp - t1.purchase_timestamp::timestamp
            )
        ) AS qty_base_days
    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    WHERE purchase_timestamp < '2018-01-01'
    AND t2.seller_id IS NOT NULL
    GROUP BY t2.seller_id
),
tb_dtorder AS (
    SELECT DISTINCT
        seller_id,
        date(purchase_timestamp) AS order_date
    FROM tb_join
    ORDER BY 1, 2
),
tb_lag AS (
    SELECT
        *,
        lag(order_date) OVER (PARTITION BY seller_id ORDER BY order_date) AS lag1
    FROM tb_dtorder
),
tb_interval AS (
    SELECT
        seller_id,
        avg(
            date_part(
                'day', order_date::timestamp - lag1::timestamp
            )
        ) AS avg_order_interval
    FROM tb_lag
    GROUP BY seller_id
)
SELECT
    date('2018-01-01') AS date_reference,
    t1.*,
    t2.max_order_price,
    t2.min_order_price,
    t3.LTV,
    t3.qty_base_days,
    t4.avg_order_interval

FROM tb_summary AS t1
LEFT JOIN tb_min_max AS t2
ON t1.seller_id = t2.seller_id
LEFT JOIN tb_customer_life AS t3
ON t1.seller_id = t3.seller_id
LEFT JOIN tb_interval AS t4
ON t1.seller_id = t4.seller_id;
