--creating temporary raw customer table that will store our raw csv data
CREATE TABLE raw_superstore (
    row_id TEXT,
    order_id TEXT,
    order_date TEXT,
    ship_date TEXT,
    ship_mode TEXT,
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    product_id TEXT,
    category TEXT,
    sub_category TEXT,
    product_name TEXT,
    sales TEXT,
    quantity TEXT,
    discount TEXT,
    profit TEXT
);
--LOAD CSV INTO raw_superstore
COPY raw_superstore
FROM 'C:/data/raw_salesData.csv'
DELIMITER ','
CSV HEADER;

SELECT COUNT(*) FROM raw_superstore;

--creating a clean staging table(clean version with proper data type)
CREATE TABLE stg_superstore_clean AS
SELECT
    row_id::INT                                   AS row_id,
    order_id                                     AS order_id,
    TO_DATE(order_date, 'DD-MM-YYYY')             AS order_date,
    TO_DATE(ship_date, 'DD-MM-YYYY')              AS ship_date,
    ship_mode                                    AS ship_mode,
    customer_id                                  AS customer_id,
    customer_name                                AS customer_name,
    segment                                      AS segment,
    country                                      AS country,
    city                                         AS city,
    state                                        AS state,
    postal_code                                  AS postal_code,
    region                                       AS region,
    product_id                                   AS product_id,
    category                                     AS category,
    sub_category                                 AS sub_category,
    product_name                                 AS product_name,
    sales::NUMERIC(10,2)                          AS sales,
    quantity::INT                                AS quantity,
    discount::NUMERIC(4,2)                        AS discount,
    profit::NUMERIC(10,2)                         AS profit
FROM raw_superstore;

SELECT COUNT(*) FROM stg_superstore_clean;

SELECT * FROM stg_superstore_clean LIMIT 5;

SELECT
    COUNT(*) FILTER (WHERE order_date IS NULL) AS null_order_dates,
    COUNT(*) FILTER (WHERE sales IS NULL)      AS null_sales
FROM stg_superstore_clean;

--Split clean data into:raw_orders,raw_customers,raw_products,raw_order_items
CREATE TABLE raw_orders AS
SELECT DISTINCT
    order_id,
    order_date,
    ship_date,
    ship_mode,
    country,
    city,
    state,
    postal_code,
    region
FROM stg_superstore_clean;

SELECT COUNT(*) FROM raw_customers;

SELECT * FROM raw_orders LIMIT 5;

CREATE TABLE raw_customers AS
SELECT DISTINCT
    customer_id,
    customer_name,
    segment
FROM stg_superstore_clean;

CREATE TABLE raw_products AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    sub_category
FROM stg_superstore_clean;

CREATE TABLE raw_order_items AS
SELECT
    order_id,
    product_id,
    sales,
    quantity,
    discount,
    profit
FROM stg_superstore_clean;

SELECT
    (SELECT COUNT(*) FROM raw_orders)       AS orders,
    (SELECT COUNT(*) FROM raw_customers)    AS customers,
    (SELECT COUNT(*) FROM raw_products)     AS products,
    (SELECT COUNT(*) FROM raw_order_items)  AS order_items;

--BUILDING ORDER-LEVEL AGGREGATION by converting order × product rows into one row per order per customer.
CREATE TABLE order_level_base AS
SELECT
    s.customer_id,
    o.order_id,
    o.order_date,
    SUM(oi.sales)                AS order_revenue,
    SUM(oi.quantity)             AS total_quantity,
    SUM(oi.sales * oi.discount) AS discount_amount,
    SUM(oi.profit)               AS profit
FROM stg_superstore_clean s
JOIN raw_orders o
  ON s.order_id = o.order_id
JOIN raw_order_items oi
  ON s.order_id = oi.order_id
 AND s.product_id = oi.product_id
GROUP BY
    s.customer_id,
    o.order_id,
    o.order_date;

SELECT COUNT(*) FROM order_level_base;

SELECT * FROM order_level_base LIMIT 50;
 
--final fact table
CREATE TABLE fact_customer_orders AS
SELECT
    customer_id,
    order_id,
    order_date,
    order_revenue,
    total_quantity,
    discount_amount,
    profit,

    -- purchase sequence
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_date, order_id
    ) AS order_number,

    -- gap between purchases
    order_date
      - LAG(order_date) OVER (
            PARTITION BY customer_id
            ORDER BY order_date, order_id
        ) AS days_since_last_order,

    -- acquisition flag
    CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date, order_id
        ) = 1
        THEN TRUE
        ELSE FALSE
    END AS is_first_order

FROM order_level_base;

SELECT COUNT(*) FROM fact_customer_orders;

-- no duplicate customer-order rows
SELECT customer_id, order_id, COUNT(*)
FROM fact_customer_orders
GROUP BY customer_id, order_id
HAVING COUNT(*) > 1;

-- first orders must have NULL gap
SELECT *
FROM fact_customer_orders
WHERE is_first_order = TRUE
  AND days_since_last_order IS NOT NULL;

--This step answers who the customer is over time, not just what they bought.
CREATE TABLE dim_customers AS
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,

    MIN(f.order_date) AS first_order_date,
    MAX(f.order_date) AS last_order_date,

    COUNT(f.order_id) AS total_orders,

    CURRENT_DATE - MIN(f.order_date) AS customer_age_days,

    CASE
        WHEN MAX(f.order_date) >= CURRENT_DATE - INTERVAL '90 days'
        THEN 'Active'
        ELSE 'Churned'
    END AS customer_status

FROM raw_customers c
JOIN fact_customer_orders f
  ON c.customer_id = f.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    c.segment;
SELECT COUNT(*) FROM dim_customers;
SELECT * FROM dim_customers LIMIT 50;
SELECT MAX(order_date) AS dataset_last_date
FROM fact_customer_orders;

DROP TABLE IF EXISTS dim_customers;
CREATE TABLE dim_customers AS
WITH dataset_anchor AS (
    SELECT MAX(order_date) AS anchor_date
    FROM fact_customer_orders
)
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,

    MIN(f.order_date) AS first_order_date,
    MAX(f.order_date) AS last_order_date,

    COUNT(f.order_id) AS total_orders,

    a.anchor_date - MIN(f.order_date) AS customer_age_days,

    CASE
        WHEN MAX(f.order_date) >= a.anchor_date - INTERVAL '90 days'
        THEN 'Active'
        ELSE 'Churned'
    END AS customer_status

FROM raw_customers c
JOIN fact_customer_orders f
  ON c.customer_id = f.customer_id
CROSS JOIN dataset_anchor a
GROUP BY
    c.customer_id,
    c.customer_name,
    c.segment,
    a.anchor_date;
SELECT customer_status, COUNT(*)
FROM dim_customers
GROUP BY customer_status;

--
CREATE TABLE customer_behavior_summary AS
WITH base AS (
    SELECT
        f.customer_id,
        COUNT(f.order_id) AS total_orders,
        SUM(f.order_revenue) AS total_revenue,
        AVG(f.order_revenue) AS avg_order_value,
        AVG(f.days_since_last_order) AS avg_days_between_orders,
        MIN(f.order_date) AS first_order_date,
        MAX(f.order_date) AS last_order_date
    FROM fact_customer_orders f
    GROUP BY f.customer_id
)
SELECT
    b.customer_id,
    b.total_orders,
    b.total_revenue,
    b.avg_order_value,
    b.avg_days_between_orders,
    b.first_order_date,
    b.last_order_date,
    d.customer_status,

    CASE
        WHEN b.total_revenue >= 3000 THEN 'High Value'
        WHEN b.total_revenue >= 1000 THEN 'Mid Value'
        ELSE 'Low Value'
    END AS value_segment

FROM base b
JOIN dim_customers d
  ON b.customer_id = d.customer_id;

SELECT COUNT(*) FROM customer_behavior_summary;
SELECT * FROM customer_behavior_summary LIMIT 100;

DROP TABLE IF EXISTS customer_behavior_summary;

CREATE TABLE customer_behavior_summary AS
WITH base AS (
    SELECT
        f.customer_id,
        COUNT(f.order_id)                           AS total_orders,
        SUM(f.order_revenue)                        AS total_revenue,
        AVG(f.order_revenue)                        AS avg_order_value_raw,
        AVG(f.days_since_last_order)                AS avg_days_between_orders_raw,
        MIN(f.order_date)                           AS first_order_date,
        MAX(f.order_date)                           AS last_order_date
    FROM fact_customer_orders f
    GROUP BY f.customer_id
)
SELECT
    b.customer_id,
    b.total_orders,
    ROUND(b.total_revenue, 2)                      AS total_revenue,
    ROUND(b.avg_order_value_raw, 2)                AS avg_order_value,
    ROUND(b.avg_days_between_orders_raw, 1)        AS avg_days_between_orders,
    b.first_order_date,
    b.last_order_date,
    d.customer_status,

    CASE
        WHEN b.total_revenue >= 3000 THEN 'High Value'
        WHEN b.total_revenue >= 1000 THEN 'Mid Value'
        ELSE 'Low Value'
    END AS value_segment

FROM base b
JOIN dim_customers d
  ON b.customer_id = d.customer_id;

--Who are the top 20% customers by Lifetime Value (LTV)?
WITH ranked_customers AS (
    SELECT
        customer_id,
        total_revenue,
        NTILE(5) OVER (ORDER BY total_revenue DESC) AS revenue_bucket
    FROM customer_behavior_summary
)
SELECT *
FROM ranked_customers
WHERE revenue_bucket = 1;

--How many customers are one-time buyers?
SELECT
    COUNT(*) AS one_time_customers
FROM customer_behavior_summary
WHERE total_orders = 1;

--Active vs Churned customers distribution
SELECT
    customer_status,
    COUNT(*) AS customers
FROM customer_behavior_summary
GROUP BY customer_status;

--Do high-value customers buy more frequently?
SELECT
    value_segment,
    ROUND(AVG(avg_days_between_orders), 1) AS avg_purchase_gap
FROM customer_behavior_summary
GROUP BY value_segment;

--Which segments generate the highest LTV?
SELECT
    d.segment,
    ROUND(AVG(c.total_revenue), 2) AS avg_ltv
FROM customer_behavior_summary c
JOIN dim_customers d
  ON c.customer_id = d.customer_id
GROUP BY d.segment;

--Identify at-risk high-value customers
SELECT *
FROM customer_behavior_summary
WHERE value_segment = 'High Value'
  AND customer_status = 'Churned';
