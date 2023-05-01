DROP TABLE IF EXISTS analytics.abt_olist_churn;
CREATE TABLE analytics.abt_olist_churn AS
WITH tb_features AS (
SELECT
    t1.date_reference,
    t1.date_ingestion,
    t1.seller_id,
    t1.qty_orders,
    t1.qty_days,
    t1.qty_items,
    t1.qty_recency,
    t1.avg_ticket,
    t1.avg_price,
    t1.max_price,
    t1.min_price,
    t1.avg_no_products,
    t1.max_order_price,
    t1.min_order_price,
    t1.ltv,
    t1.qty_base_days,
    t1.avg_order_interval,
    t2.pct_delayed,
    t2.pct_cancelled,
    t2.avg_freight,
    t2.median_freight,
    t2.max_freight,
    t2.min_freight,
    t2.avg_delivery_time_approved,
    t2.avg_delivery_time_ordered,
    t2.avg_expected_delivery_days,
    t3.qty_debit_card_orders,
    t3.qty_credit_card_orders,
    t3.qty_voucher_orders,
    t3.qty_boleto_orders,
    t3.pct_debit_card_orders,
    t3.pct_credit_card_orders,
    t3.pct_voucher_orders,
    t3.pct_boleto_orders,
    t3.debit_card_qty_orders,
    t3.credit_card_qty_orders,
    t3.voucher_qty_orders,
    t3.pct_boleto_value,
    t3.pct_debit_card_value,
    t3.pct_credit_card_value,
    t3.pct_voucher_value,
    t3.avg_installments,
    t3.median_installments,
    t3.max_installments,
    t3.min_installments,
    t4.avg_photos,
    t4.avg_product_volume,
    t4.median_product_volume,
    t4.max_product_volume,
    t4.min_product_volume,
    t4.pct_category_cama_mesa_banho,
    t4.pct_category_beleza_saude,
    t4.pct_category_esporte_lazer,
    t4.pct_category_informatica_acessorios,
    t4.pct_category_moveis_decoracao,
    t4.pct_category_utilidades_domesticas,
    t4.pct_category_relogios_presentes,
    t4.pct_category_telefonia,
    t4.pct_category_automotivo,
    t4.pct_category_brinquedos,
    t5.avg_score,
    t5.median_score,
    t5.min_score,
    t5.max_score,
    t5.pct_review,
    t6.qty_state_order,
    t6.pct_order_ac,
    t6.pct_order_al,
    t6.pct_order_am,
    t6.pct_order_ap,
    t6.pct_order_ba,
    t6.pct_order_ce,
    t6.pct_order_df,
    t6.pct_order_es,
    t6.pct_order_go,
    t6.pct_order_ma,
    t6.pct_order_mg,
    t6.pct_order_ms,
    t6.pct_order_mt,
    t6.pct_order_pa,
    t6.pct_order_pb,
    t6.pct_order_pe,
    t6.pct_order_pi,
    t6.pct_order_pr,
    t6.pct_order_rj,
    t6.pct_order_rn,
    t6.pct_order_ro,
    t6.pct_order_rr,
    t6.pct_order_rs,
    t6.pct_order_sc,
    t6.pct_order_se,
    t6.pct_order_to,
    t6.pct_order_sp
FROM analytics.fs_sale AS t1
LEFT JOIN analytics.fs_delivery AS t2
ON t1.seller_id = t2.seller_id
AND t1.date_reference = t2.date_reference
LEFT JOIN analytics.fs_payment AS t3
ON t1.seller_id = t3.seller_id
AND t1.date_reference = t3.date_reference
LEFT JOIN analytics.fs_product AS t4
ON t1.seller_id = t4.seller_id
AND t1.date_reference = t4.date_reference
LEFT JOIN analytics.fs_review AS t5
ON t1.seller_id = t5.seller_id
AND t1.date_reference = t5.date_reference
LEFT JOIN analytics.fs_client AS t6
ON t1.seller_id = t6.seller_id
AND t1.date_reference = t6.date_reference
WHERE t1.qty_recency <= 45
),
tb_event AS (
    SELECT DISTINCT
        seller_id,
        date(purchase_timestamp) AS activation_date
    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    WHERE seller_id IS NOT NULL
),
tb_flag AS (
    SELECT
        t1.seller_id,
        t1.date_reference,
        min(t2.activation_date) AS first_activation_date
    FROM tb_features AS t1
    LEFT JOIN tb_event AS t2
    ON t1.seller_id = t2.seller_id
    AND t1.date_reference < t2.activation_date
    AND date_part(
        'day',
        t2.activation_date::timestamp - t1.date_reference::timestamp
    ) <= 45 - t1.qty_recency
    GROUP BY t1.seller_id, t1.date_reference
)
SELECT
    t1.*,
    CASE WHEN t2.first_activation_date IS NULL THEN 1 ELSE 0 END AS churn
FROM tb_features AS t1
LEFT JOIN tb_flag AS t2
ON t1.seller_id = t2.seller_id
AND t1.date_reference = t2.date_reference
ORDER BY t1.seller_id, t1.date_reference;