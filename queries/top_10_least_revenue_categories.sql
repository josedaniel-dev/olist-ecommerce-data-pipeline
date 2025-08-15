SELECT 
    p.product_category_name AS Category,
    COUNT(DISTINCT oi.order_id) AS Num_order,
    SUM(oi.price + oi.freight_value) AS Revenue
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id
JOIN olist_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
  AND p.product_category_name IS NOT NULL
GROUP BY Category
ORDER BY Revenue ASC, Category ASC
LIMIT 10;
