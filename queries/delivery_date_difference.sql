WITH diffs AS (
  SELECT
    c.customer_state AS State,
    -- estimado - real (positivo = se entregó antes del estimado)
    (julianday(date(o.order_estimated_delivery_date)) - julianday(date(o.order_delivered_customer_date))) AS diff_days
  FROM olist_orders o
  JOIN olist_customers c ON c.customer_id = o.customer_id
  WHERE o.order_delivered_customer_date IS NOT NULL
)
SELECT
  State,
  CAST(AVG(diff_days) AS INTEGER) AS Delivery_Difference
FROM diffs
GROUP BY State
ORDER BY Delivery_Difference ASC, State ASC;
