SELECT 
    strftime('%m', o.order_purchase_timestamp) AS month_no,
    CASE strftime('%m', o.order_purchase_timestamp)
        WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
        WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
        WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
        WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
    END AS month,
    SUM(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2016' THEN (oi.price + oi.freight_value) ELSE 0 END) AS Year2016,
    SUM(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2017' THEN (oi.price + oi.freight_value) ELSE 0 END) AS Year2017,
    SUM(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2018' THEN (oi.price + oi.freight_value) ELSE 0 END) AS Year2018
FROM olist_orders o
JOIN olist_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY month_no, month
ORDER BY month_no;
