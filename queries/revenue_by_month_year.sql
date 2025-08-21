SELECT month as month_no,
       CASE
           WHEN a.month='01' THEN 'Jan'
           WHEN a.month='02' THEN 'Feb'
           WHEN a.month='03' THEN 'Mar'
           WHEN a.month='04' THEN 'Apr'
           WHEN a.month='05' THEN 'May'
           WHEN a.month='06' THEN 'Jun'
           WHEN a.month='07' THEN 'Jul'
           WHEN a.month='08' THEN 'Aug'
           WHEN a.month='09' THEN 'Sep'
           WHEN a.month='10' THEN 'Oct'
           WHEN a.month='11' THEN 'Nov'
           WHEN a.month='12' THEN 'Dec'
           ELSE 0
       END AS month,
       SUM(CASE WHEN a.year= '2016' THEN payment_value ELSE 0 END) AS Year2016,
       SUM(CASE WHEN a.year= '2017' THEN payment_value ELSE 0 END) AS Year2017,
       SUM(CASE WHEN a.year= '2018' THEN payment_value ELSE 0 END) AS Year2018
FROM (
    SELECT customer_id,
           order_id,
           order_delivered_customer_date,
           order_status,
           strftime('%Y', order_delivered_customer_date) AS Year,
           strftime('%m', order_delivered_customer_date) AS Month,
           payment_value
    FROM olist_orders
    JOIN olist_order_payments USING (order_id)
    WHERE order_status = 'delivered'
      AND order_delivered_customer_date IS NOT NULL
    GROUP BY 1,2,3
    ORDER BY order_delivered_customer_date ASC
) a
GROUP BY month
ORDER BY month_no ASC;
