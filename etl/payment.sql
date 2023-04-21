DROP TABLE IF EXISTS analytics.fs_payment;
CREATE TABLE analytics.fs_payment AS
WITH
tb_order AS (
    SELECT DISTINCT t1.order_id, t2.seller_id
    FROM orders AS t1
    LEFT JOIN order_items AS t2
    ON t1.order_id = t2.order_id
    WHERE purchase_timestamp < '2018-01-01'
    AND purchase_timestamp >= date('2018-01-01') - interval '6 months'
    AND seller_id IS NOT NULL
),
tb_join AS (
    SELECT t1.seller_id, t2.*
    FROM tb_order AS t1
        LEFT JOIN order_payments AS t2 ON t1.order_id = t2.order_id
),
tb_group AS (
    SELECT
        seller_id,
        payment_type,
        count(DISTINCT order_id) AS order_qty_payment_type,
        sum(payment_value) AS order_value_payment_type
    FROM tb_join
    GROUP BY seller_id, payment_type
    ORDER BY seller_id, payment_type
),
tb_summary AS (
    SELECT DISTINCT seller_id,
        -- count per payment type
        sum(
            CASE WHEN payment_type = 'debit_card'
            THEN order_qty_payment_type ELSE 0 END
        ) AS qty_debit_card_orders,
        sum(
            CASE WHEN payment_type = 'credit_card'
            THEN order_qty_payment_type ELSE 0 END
        ) AS qty_credit_card_orders,
        sum(
            CASE WHEN payment_type = 'voucher'
            THEN order_qty_payment_type ELSE 0 END
        ) AS qty_voucher_orders,
        sum(
            CASE WHEN payment_type = 'boleto'
            THEN order_qty_payment_type ELSE 0 END
        ) AS qty_boleto_orders,
        -- proportion of payment type
        sum(
            CASE WHEN payment_type = 'debit_card'
            THEN order_qty_payment_type ELSE 0 END
        ) / sum(order_qty_payment_type) AS pct_debit_card_orders,
        sum(
            CASE WHEN payment_type = 'credit_card'
            THEN order_qty_payment_type ELSE 0 END
        ) / sum(order_qty_payment_type) AS pct_credit_card_orders,
        sum(
            CASE WHEN payment_type = 'voucher'
            THEN order_qty_payment_type ELSE 0 END
        ) / sum(order_qty_payment_type) AS pct_voucher_orders,
        sum(
            CASE WHEN payment_type = 'boleto'
            THEN order_qty_payment_type ELSE 0 END
        ) / sum(order_qty_payment_type) AS pct_boleto_orders,
        -- total value per payment type
        sum(
            CASE WHEN payment_type = 'debit_card'
            THEN order_value_payment_type ELSE 0 END
        ) AS debit_card_qty_orders,
        sum(
            CASE WHEN payment_type = 'credit_card'
            THEN order_value_payment_type ELSE 0 END
        ) AS credit_card_qty_orders,
        sum(
            CASE WHEN payment_type = 'voucher'
            THEN order_value_payment_type ELSE 0 END
        ) AS voucher_qty_orders,
        -- proportion of total value per payment type
        sum(
            CASE WHEN payment_type = 'boleto'
            THEN order_value_payment_type ELSE 0 END
        ) / sum(order_value_payment_type) AS pct_boleto_value,
        sum(
            CASE WHEN payment_type = 'debit_card'
            THEN order_value_payment_type ELSE 0 END
        ) / sum(order_value_payment_type) AS pct_debit_card_value,
        sum(
            CASE WHEN payment_type = 'credit_card'
            THEN order_value_payment_type ELSE 0 END
        ) / sum(order_value_payment_type) AS pct_credit_card_value,
        sum(
            CASE WHEN payment_type = 'voucher'
            THEN order_value_payment_type ELSE 0 END
        ) / sum(order_value_payment_type) AS pct_voucher_value
    FROM tb_group
    GROUP BY seller_id
),
tb_installments AS (
    SELECT 
        seller_id,
        avg(payment_installments) AS avg_installments,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY payment_installments) AS median_installments,
        max(payment_installments) AS max_installments,
        min(payment_installments) AS min_installments
    FROM tb_join
    WHERE payment_type = 'credit_card'
    GROUP BY seller_id
)
SELECT
    date('2018-01-01') as date_reference,
    t1.*,
    t2.avg_installments,
    t2.median_installments,
    t2.max_installments,
    t2.min_installments
FROM tb_summary AS t1
LEFT JOIN tb_installments AS t2
ON t1.seller_id = t2.seller_id;