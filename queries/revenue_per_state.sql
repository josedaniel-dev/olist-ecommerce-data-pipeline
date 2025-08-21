WITH delivered AS (
  SELECT DISTINCT
    o.order_id,
    c.customer_state
  FROM olist_orders AS o
  JOIN olist_customers AS c
    ON c.customer_id = o.customer_id
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
),
pay AS (
  SELECT
    op.order_id,
    SUM(CAST(op.payment_value AS REAL)) AS pay_amt
  FROM olist_order_payments AS op
  GROUP BY op.order_id
)
SELECT
  d.customer_state,
  SUM(p.pay_amt) AS Revenue
FROM delivered d
JOIN pay p ON p.order_id = d.order_id
GROUP BY d.customer_state
ORDER BY Revenue DESC, d.customer_state ASC
LIMIT 10;
