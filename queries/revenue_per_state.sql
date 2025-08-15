SELECT 
    c.customer_state,
    SUM(oi.price + oi.freight_value) AS Revenue
FROM olist_orders o
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN olist_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY Revenue DESC
LIMIT 10;
