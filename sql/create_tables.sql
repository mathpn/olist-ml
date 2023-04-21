CREATE TABLE IF NOT EXISTS order_payments (
    order_id text,
    payment_sequential integer,
    payment_type text,
    payment_installments integer,
    payment_value real,
    PRIMARY KEY (order_id, payment_sequential)
);
CREATE TABLE IF NOT EXISTS orders (
    order_id text,
    customer_id text,
    order_status text,
    purchase_timestamp timestamp,
    approved_at timestamp,
    date_sent timestamp,
    delivery_date timestamp,
    estimated_delivery_date timestamp,
    PRIMARY KEY (order_id)
);
CREATE TABLE IF NOT EXISTS products (
    product_id text,
    category_name text,
    name_length real,
    description_length real,
    photos_qty real,
    weight_g real,
    length_cm real,
    height_cm real,
    width_cm real,
    PRIMARY KEY (product_id)
);
CREATE TABLE IF NOT EXISTS customers (
    customer_id text,
    customer_unique_id text,
    code_zip bigint,
    desc_city text,
    desc_state text,
    PRIMARY KEY (customer_id)
);
CREATE TABLE IF NOT EXISTS geolocation (
    code_zip bigint,
    nr_latitude real,
    nr_longitude real,
    desc_city text,
    desc_state text
);
CREATE TABLE IF NOT EXISTS order_items (
    order_id text,
    order_item_id bigint,
    product_id text,
    seller_id text,
    shipping_limit_date timestamp,
    price real,
    freight_value real,
    PRIMARY KEY (order_id, order_item_id)
);
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id text,
    order_id text,
    review_score real,
    desc_comment_title text,
    desc_comment_message text,
    review_date timestamp,
    answer_date timestamp,
    PRIMARY KEY (review_id, order_id)
);
CREATE TABLE IF NOT EXISTS sellers (
    seller_id text,
    code_zip bigint,
    desc_city text,
    desc_state text,
    PRIMARY KEY (seller_id)
);
CREATE SCHEMA analytics;