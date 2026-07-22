-- 1. Busiest Hours: Most active hours of the day for orders
SELECT 'Busiest_Hours' AS query_name, CAST(order_hour_of_day AS STRING) AS category, COUNT(1) AS metric_value
FROM orders
GROUP BY order_hour_of_day;

-- 2. Busiest Days: Most active days of the week for orders
SELECT 'Busiest_Days' AS query_name, CAST(order_dow AS STRING) AS category, COUNT(1) AS metric_value
FROM orders
GROUP BY order_dow;

-- 3. Top 10 Products: Most frequently purchased products
SELECT 'Top_10_Products' AS query_name, p.product_name AS category, COUNT(op.product_id) AS metric_value
FROM order_products_prior op
JOIN products p ON op.product_id = p.product_id
GROUP BY p.product_name
ORDER BY metric_value DESC
LIMIT 10;

-- 4. Top Departments: Departments with the highest number of sales
SELECT 'Top_Departments' AS query_name, d.department AS category, COUNT(op.product_id) AS metric_value
FROM order_products_prior op
JOIN products p ON op.product_id = p.product_id
JOIN departments d ON p.department_id = d.department_id
GROUP BY d.department;

-- 5. Department Reorder Ratio: Reorder ratio for each department
SELECT 'Reorder_Ratio_Dept' AS query_name, d.department AS category, (SUM(op.reordered) / COUNT(op.product_id)) AS metric_value
FROM order_products_prior op
JOIN products p ON op.product_id = p.product_id
JOIN departments d ON p.department_id = d.department_id
GROUP BY d.department;

-- 6. Top 10 Aisles: Aisles with the highest number of sales
SELECT 'Top_10_Aisles' AS query_name, a.aisle AS category, COUNT(op.product_id) AS metric_value
FROM order_products_prior op
JOIN products p ON op.product_id = p.product_id
JOIN aisles a ON p.aisle_id = a.aisle_id
GROUP BY a.aisle
ORDER BY metric_value DESC
LIMIT 10;

-- 7. Aisle Reorder Ratio: Reorder ratio for each aisle
SELECT 'Reorder_Ratio_Aisle' AS query_name, a.aisle AS category, (SUM(op.reordered) / COUNT(op.product_id)) AS metric_value
FROM order_products_prior op
JOIN products p ON op.product_id = p.product_id
JOIN aisles a ON p.aisle_id = a.aisle_id
GROUP BY a.aisle;

-- 8. Top Reordered Products: Products with the highest reorder counts
SELECT 'Top_Reordered_Products' AS query_name, p.product_name AS category, SUM(op.reordered) AS metric_value
FROM order_products_prior op
JOIN products p ON op.product_id = p.product_id
GROUP BY p.product_name
ORDER BY metric_value DESC
LIMIT 10;

-- 9. Order Frequency: Distribution of days since prior order
--    Fix: exclude first orders (is_first_order=true) where PySpark filled the null with 0.0
--    Fix: label the 30.0 cap artifact separately so it is not mistaken for real behavior
SELECT 'Days_Since_Prior_Order' AS query_name,
    CASE
        WHEN days_capped = true THEN '30+ (capped)'
        ELSE CAST(CAST(days_since_prior_order AS INT) AS STRING)
    END AS category,
    COUNT(1) AS metric_value
FROM orders
WHERE is_first_order = false
GROUP BY
    CASE
        WHEN days_capped = true THEN '30+ (capped)'
        ELSE CAST(CAST(days_since_prior_order AS INT) AS STRING)
    END;

-- 10. User Engagement Distribution: how many users have each order count
--     Fix: the old query returned 10 users all tied at 100 (the dataset's artificial cap)
--     This version shows the real spread of engagement across all 206,209 users
SELECT 'User_Order_Distribution' AS query_name,
    CAST(total_orders AS STRING) AS category,
    COUNT(user_id) AS metric_value
FROM (
    SELECT user_id, COUNT(order_id) AS total_orders
    FROM orders
    WHERE eval_set = 'prior'
    GROUP BY user_id
) user_counts
GROUP BY total_orders
ORDER BY total_orders;
