-- Monthly avg delivery times (real vs estimated) from purchase date, delivered only
WITH base AS (
  SELECT
    printf('%02d', CAST(strftime('%m', o.order_purchase_timestamp) AS INTEGER)) AS month_no,
    CASE strftime('%m', o.order_purchase_timestamp)
      WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
      WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
      WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
      WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
    END AS month,
    strftime('%Y', o.order_purchase_timestamp) AS yr,
    (julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)) AS real_days,
    (julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp)) AS est_days
  FROM delivered_orders o
  WHERE o.order_purchase_timestamp IS NOT NULL
),
agg AS (
  SELECT
    month_no, month, yr,
    AVG(real_days)       AS real_time,
    AVG(est_days)        AS estimated_time
  FROM base
  GROUP BY month_no, month, yr
)
SELECT
  month_no,
  month,
  COALESCE(SUM(CASE WHEN yr = '2016' THEN real_time END), 0) AS Year2016_real_time,
  COALESCE(SUM(CASE WHEN yr = '2017' THEN real_time END), 0) AS Year2017_real_time,
  COALESCE(SUM(CASE WHEN yr = '2018' THEN real_time END), 0) AS Year2018_real_time,
  COALESCE(SUM(CASE WHEN yr = '2016' THEN estimated_time END), 0) AS Year2016_estimated_time,
  COALESCE(SUM(CASE WHEN yr = '2017' THEN estimated_time END), 0) AS Year2017_estimated_time,
  COALESCE(SUM(CASE WHEN yr = '2018' THEN estimated_time END), 0) AS Year2018_estimated_time
FROM agg
GROUP BY month_no, month
ORDER BY month_no;
