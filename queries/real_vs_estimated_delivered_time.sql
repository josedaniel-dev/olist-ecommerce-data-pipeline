SELECT 
    strftime('%m', o.order_purchase_timestamp) AS month_no,
    CASE strftime('%m', o.order_purchase_timestamp)
        WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
        WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
        WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
        WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
    END AS month,
    ROUND(AVG(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2016' 
        THEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) END), 2) AS Year2016_real_time,
    ROUND(AVG(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2017' 
        THEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) END), 2) AS Year2017_real_time,
    ROUND(AVG(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2018' 
        THEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) END), 2) AS Year2018_real_time,
    ROUND(AVG(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2016' 
        THEN julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp) END), 2) AS Year2016_estimated_time,
    ROUND(AVG(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2017' 
        THEN julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp) END), 2) AS Year2017_estimated_time,
    ROUND(AVG(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2018' 
        THEN julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp) END), 2) AS Year2018_estimated_time
FROM olist_orders o
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL
GROUP BY month_no, month
ORDER BY month_no;
