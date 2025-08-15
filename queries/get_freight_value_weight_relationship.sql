SELECT 
    oi.order_id,
    SUM(p.product_weight_g) AS total_weight_g,
    SUM(oi.freight_value) AS total_freight_value
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id
GROUP BY oi.order_id
ORDER BY oi.order_id;
