WITH tb_order AS (
    SELECT DISTINCT
        t1.order_id,
        t2.seller_id
    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    WHERE purchase_timestamp < '{date}'
    AND purchase_timestamp >= date('{date}') - interval '6 months'
    AND seller_id IS NOT NULL
),
tb_join AS (
    SELECT
        t1.order_id,
        t2.review_score
    FROM tb_order AS t1
    LEFT JOIN order_reviews AS t2
    ON t1.order_id = t2.order_id
),
tb_summary AS (
    SELECT
        seller_id,
        avg(review_score) AS avg_score,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY (review_score)) AS median_score,
        min(review_score) AS min_score,
        max(review_score) AS max_score,
        COUNT(review_score)::decimal / COUNT(t1.order_id) AS pct_review
    FROM tb_order AS t1
    LEFT JOIN tb_join AS t2
    ON t1.order_id = t2.order_id
    GROUP BY seller_id
)
SELECT
    date('{date}') as date_reference,
    NOW() AS date_ingestion,
    *
FROM tb_summary;