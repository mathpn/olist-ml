DROP TABLE IF EXISTS analytics.fs_product;
CREATE TABLE analytics.fs_product AS
WITH tb_join AS (
    SELECT
        DISTINCT
        t2.seller_id,
        t3.*
    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    LEFT JOIN products AS t3
    ON t2.product_id = t3.product_id
    WHERE purchase_timestamp < '2018-01-01'
    AND purchase_timestamp >= date('2018-01-01') - interval '6 months'
    AND t2.seller_id IS NOT NULL
),
tb_summary AS (
    SELECT
        seller_id,
        avg(coalesce(photos_qty, 0)) AS avg_photos,
        avg(length_cm * height_cm * width_cm) AS avg_product_volume,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY (length_cm * height_cm * width_cm)) AS median_product_volume,
        max(length_cm * height_cm * width_cm) AS max_product_volume,
        min(length_cm * height_cm * width_cm) AS min_product_volume,
        COUNT(DISTINCT CASE WHEN category_name = 'category_name' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_category_name,
        COUNT(DISTINCT CASE WHEN category_name = 'cama_mesa_banho' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_cama_mesa_banho,
        COUNT(DISTINCT CASE WHEN category_name = 'beleza_saude' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_beleza_saude,
        COUNT(DISTINCT CASE WHEN category_name = 'esporte_lazer' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_esporte_lazer,
        COUNT(DISTINCT CASE WHEN category_name = 'informatica_acessorios' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_informatica_acessorios,
        COUNT(DISTINCT CASE WHEN category_name = 'moveis_decoracao' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_moveis_decoracao,
        COUNT(DISTINCT CASE WHEN category_name = 'utilidades_domesticas' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_utilidades_domesticas,
        COUNT(DISTINCT CASE WHEN category_name = 'relogios_presentes' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_relogios_presentes,
        COUNT(DISTINCT CASE WHEN category_name = 'telefonia' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_telefonia,
        COUNT(DISTINCT CASE WHEN category_name = 'automotivo' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_automotivo,
        COUNT(DISTINCT CASE WHEN category_name = 'brinquedos' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_brinquedos,
        COUNT(DISTINCT CASE WHEN category_name = 'cool_stuff' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_cool_stuff,
        COUNT(DISTINCT CASE WHEN category_name = 'ferramentas_jardim' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_ferramentas_jardim,
        COUNT(DISTINCT CASE WHEN category_name = 'perfumaria' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_perfumaria,
        COUNT(DISTINCT CASE WHEN category_name = 'bebes' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_bebes,
        COUNT(DISTINCT CASE WHEN category_name = 'eletronicos' THEN product_id END)::decimal / COUNT(DISTINCT product_id) AS pct_category_eletronicos
    FROM tb_join
    GROUP BY seller_id
)
SELECT 
    date('2018-01-01') as date_reference,
    *
FROM tb_summary;