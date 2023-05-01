WITH tb_order AS (
    SELECT DISTINCT
        t1.order_id,
        t2.seller_id,
        t1.order_status,
        t1.purchase_timestamp,
        t1.approved_at,
        t1.delivery_date,
        t1.estimated_delivery_date,
        SUM(t2.freight_value) AS total_freight

    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    WHERE purchase_timestamp < '{date}'
    AND purchase_timestamp >= date('{date}') - interval '6 months'
    AND seller_id IS NOT NULL
    GROUP BY
        t1.order_id,
        t2.seller_id,
        t1.order_status,
        t1.purchase_timestamp,
        t1.approved_at,
        t1.delivery_date,
        t1.estimated_delivery_date
),
tb_summary AS (
    SELECT
        seller_id,
        COUNT(
            DISTINCT CASE WHEN coalesce(DATE(delivery_date), '{date}')
            > DATE(estimated_delivery_date)
            THEN order_id END
        )::decimal / NULLIF(
            COUNT(
                DISTINCT CASE WHEN order_status = 'delivered'
                THEN order_id END
            ),
            0
        ) AS pct_delayed,
        COUNT(
            DISTINCT CASE WHEN order_status = 'canceled'
            THEN order_id END)::decimal / COUNT(DISTINCT order_id
        ) AS pct_cancelled,
        avg(total_freight) AS avg_freight,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY total_freight) AS median_freight,
        max(total_freight) AS max_freight,
        min(total_freight) AS min_freight,
        avg(
            date_part('day', delivery_date::timestamp - approved_at::timestamp)
        ) AS avg_delivery_time_approved,
        avg(
            date_part('day', delivery_date::timestamp - purchase_timestamp::timestamp)
        ) AS avg_delivery_time_ordered,
        avg(
            date_part('day', estimated_delivery_date::timestamp - delivery_date::timestamp)
        ) AS avg_expected_delivery_days
    FROM tb_order
    GROUP BY seller_id
)
SELECT
    date('{date}') AS date_reference,
    NOW() AS date_ingestion,
    *
FROM tb_summary;