SELECT 
    DATE(o.order_purchase_timestamp) AS date,
    COUNT(*) AS orders,
    CASE WHEN ph.date IS NOT NULL THEN 1 ELSE 0 END AS is_holiday
FROM olist_orders o
LEFT JOIN public_holidays ph ON DATE(o.order_purchase_timestamp) = DATE(ph.date)
WHERE strftime('%Y', o.order_purchase_timestamp) = '2017'
GROUP BY date
ORDER BY date;
