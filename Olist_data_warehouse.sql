-- Create Staging Tables
CREATE TABLE staging_customers (
    customer_id NVARCHAR(50),
    customer_unique_id NVARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city NVARCHAR(100),
    customer_state NVARCHAR(10)
);
CREATE TABLE staging_order_items (
    order_id NVARCHAR(50),
	order_item_id TINYINT,
	product_id NVARCHAR(50),
	seller_id NVARCHAR(50),
	shipping_limit_date DATETIME,
	price FLOAT,
	freight_value FLOAT
);
CREATE TABLE staging_orders (
    order_id NVARCHAR(50),
    customer_id NVARCHAR(50),
    order_status NVARCHAR(50),
    order_purchase_timestamp DATETIME,
	order_approved_at DATETIME,
	order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
	order_estimated_delivery_date DATETIME
);
CREATE TABLE staging_products (
	product_id NVARCHAR (50),
	product_category_name NVARCHAR (50),
	product_name_lenght TINYINT,
    product_description_lenght SMALLINT,
	product_photos_qty TINYINT,
    product_weight_g INT,
    product_length_cm TINYINT,
    product_height_cm TINYINT,
    product_width_cm TINYINT
);
CREATE TABLE staging_product_category_name_translation (
    product_category_name NVARCHAR(50),
	product_category_name_english NVARCHAR(50)
);
-- Import data to Staging tables
INSERT INTO staging_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
SELECT DISTINCT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM [dbo].[olist_customers_dataset];

INSERT INTO staging_order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
SELECT DISTINCT order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
FROM [dbo].[olist_order_items_dataset];

INSERT INTO staging_orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
SELECT DISTINCT order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
FROM [dbo].[olist_orders_dataset];

INSERT INTO staging_products (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty,product_weight_g, product_length_cm, product_height_cm, product_width_cm)
SELECT DISTINCT product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty,product_weight_g, product_length_cm, product_height_cm, product_width_cm
FROM [dbo].[olist_products_dataset];

-- Update the English category name by joining with the translation table
ALTER TABLE staging_products
ADD product_category_name_english NVARCHAR(100);
UPDATE sp
SET sp.product_category_name_english = pct.product_category_name_english
FROM staging_products sp
LEFT JOIN [dbo].[product_category_name_translation] pct
ON sp.product_category_name = pct.product_category_name;

-- Check NULL values
DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = @SQL + '
SELECT 
''' + TABLE_NAME + ''' AS table_name,
''' + COLUMN_NAME + ''' AS column_name,
SUM(CASE WHEN [' + COLUMN_NAME + '] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM dbo.' + TABLE_NAME + '
UNION ALL
'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
AND TABLE_NAME LIKE 'staging_%';

SET @SQL = LEFT(@SQL, LEN(@SQL) - 10);

EXEC sp_executesql @SQL;

-- Create Dimension tables
CREATE TABLE dim_customers (
	customer_id NVARCHAR(50) PRIMARY KEY,
    customer_unique_id NVARCHAR(50),
	customer_zip_code_prefix INT,
    customer_city NVARCHAR(100),
    customer_state NVARCHAR(10)
);
CREATE TABLE dim_orders (
    order_key INT,	order_id NVARCHAR(50) PRIMARY KEY,
	order_status NVARCHAR (50)
);
CREATE TABLE dim_product (
	product_id NVARCHAR(50) PRIMARY KEY,
	product_category NVARCHAR (100),
    product_weight FLOAT,
    product_length FLOAT,
    product_height FLOAT,
    product_width FLOAT
);
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,  
    full_date DATE NOT NULL, 
    day INT,
    month INT,
	quarter INT,
    year INT
);

-- Import data to Dimension tables
INSERT INTO dim_customers (customer_id, customer_unique_id, customer_zip_code_prefix,customer_city, customer_state)
SELECT DISTINCT customer_id, customer_unique_id, customer_zip_code_prefix,customer_city, customer_state
FROM staging_customers;

INSERT INTO dim_orders (order_id,order_status)
SELECT DISTINCT order_id, order_status
FROM staging_orders;

INSERT INTO dim_product (product_id, product_category, product_weight, product_length, product_height,product_width)
SELECT DISTINCT product_id, ISNULL(product_category_name_english,'Unknown'), product_weight_g, product_length_cm, product_height_cm, product_width_cm
FROM staging_products;

INSERT INTO dim_date (date_key,full_date, day, month, quarter, year)
SELECT DISTINCT
    CONVERT(INT, FORMAT(order_purchase_timestamp,'yyyyMMdd')) AS date_key,
    CAST(order_purchase_timestamp AS DATE),
    DAY(order_purchase_timestamp),
    MONTH(order_purchase_timestamp),
    DATEPART(QUARTER, order_purchase_timestamp),
    YEAR(order_purchase_timestamp)
FROM staging_orders;

-- Check the data after loading
SELECT TOP 10 * FROM dim_customers;
SELECT TOP 10 * FROM dim_orders;
SELECT TOP 10 * FROM dim_product;
SELECT TOP 10 * FROM dim_date;

-- Create Fact table
CREATE TABLE fact_sales (
    order_id VARCHAR(50),
    order_item_id INT,
    date_key INT,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    price FLOAT,
    freight FLOAT,
    total_amount AS (price + freight),
    order_purchase DATETIME,
    order_delivered_customer DATETIME,
	 PRIMARY KEY(order_id, order_item_id)
);
-- Import data to Fact table
INSERT INTO fact_sales (
    order_id, 
    order_item_id,
	date_key,
    customer_id,
    product_id,
    price,
    freight,
	order_purchase,
	order_delivered_customer)
SELECT DISTINCT
    soi.order_id,
	soi.order_item_id,
	dd.date_key,
    dc.customer_id,
    dp.product_id,
    soi.price,
    soi.freight_value,
    so.order_purchase_timestamp,
    so.order_delivered_customer_date

FROM staging_order_items soi

JOIN staging_orders so
    ON soi.order_id = so.order_id

JOIN staging_customers sc
    ON so.customer_id = sc.customer_id

JOIN dim_orders do
    ON so.order_id = do.order_id

JOIN dim_customers dc
    ON sc.customer_id = dc.customer_id

JOIN dim_product dp
    ON soi.product_id = dp.product_id

JOIN dim_date dd
    ON CAST(so.order_purchase_timestamp AS DATE) = dd.full_date;

SELECT *
FROM fact_sales

-- create RFM customer table
CREATE TABLE rfm_customer (
    customer_unique_id NVARCHAR(50) PRIMARY KEY,
    recency INT,
    frequency INT,
    monetary FLOAT
);
INSERT INTO rfm_customer (customer_unique_id, recency, frequency, monetary)
SELECT 
    dc.customer_unique_id,
    DATEDIFF(DAY, MAX(fs.order_purchase), CAST('2018-09-01' AS DATE)) AS Recency,
    COUNT(DISTINCT fs.order_id) AS Frequency,
    SUM(fs.price + fs.freight) AS Monetary
FROM  dbo.dim_customers dc
JOIN dbo.fact_sales fs
    ON dc.customer_id = fs.customer_id
JOIN dbo.dim_orders do
    ON do.order_id = fs.order_id
WHERE do.order_status = 'delivered'
GROUP BY dc.customer_unique_id;

-- monthly order and revenue
SELECT d.year, d.month, COUNT(DISTINCT f.order_id) AS monthly_order, SUM(f.total_amount) AS monthly_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
