SELECT 
    c.customer_state AS State,
    ROUND(AVG(julianday(o.order_delivered_customer_date) - julianday(o.order_estimated_delivery_date)), 2) AS Delivery_Difference
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY State
ORDER BY State;
