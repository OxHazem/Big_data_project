-- Create and select the database namespace
CREATE DATABASE IF NOT EXISTS instacart;
USE instacart;

-- Table 1: Aisles
CREATE EXTERNAL TABLE IF NOT EXISTS aisles (
    aisle_id INT,
    aisle STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/project/data/preprocessed/aisles';

-- Table 2: Departments
CREATE EXTERNAL TABLE IF NOT EXISTS departments (
    department_id INT,
    department STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/project/data/preprocessed/departments';

-- Table 3: Products (includes your custom cold-start flags)
CREATE EXTERNAL TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    aisle_id INT,
    department_id INT,
    aisle STRING,
    department STRING,
    has_train_label BOOLEAN,
    is_cold_start_product BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/project/data/preprocessed/products';

-- Table 4: Orders (Partitioned by eval_set just like your Spark output)
CREATE EXTERNAL TABLE IF NOT EXISTS orders (
    order_id INT,
    user_id INT,
    order_number INT,
    order_dow SMALLINT,
    order_hour_of_day SMALLINT,
    days_since_prior_order FLOAT,
    is_first_order BOOLEAN,
    days_capped BOOLEAN,
    same_day_reorder BOOLEAN,
    order_dow_label STRING,
    hour_bin STRING
)
PARTITIONED BY (eval_set STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/project/data/preprocessed/orders';

-- Force Hive to scan HDFS and detect the partition subfolders
MSCK REPAIR TABLE orders;

-- Table 5: Order Products Prior
CREATE EXTERNAL TABLE IF NOT EXISTS order_products_prior (
    order_id INT,
    product_id INT,
    add_to_cart_order INT,
    reordered SMALLINT,
    basket_size BIGINT,
    is_large_basket BOOLEAN,
    user_id INT,
    eval_set STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/project/data/preprocessed/order_products_prior';

-- Table 6: Order Products Train
CREATE EXTERNAL TABLE IF NOT EXISTS order_products_train (
    order_id INT,
    product_id INT,
    add_to_cart_order INT,
    reordered SMALLINT,
    basket_size BIGINT,
    is_large_basket BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/project/data/preprocessed/order_products_train';