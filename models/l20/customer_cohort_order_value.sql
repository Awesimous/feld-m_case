with orders as(
    select * from {{ ref('stg_orders') }}
),
order_details as(
    select * from {{ ref('stg_order_details') }}
),

customer_cohort_grid as(
	select * from {{ ref('customer_cohort_grid') }}
),

customer_dim as(
	select * from {{ ref('customer_dim') }}
),

cohort_order_value AS (
    SELECT
        cd.country,
        to_char(cd.first_purchase_date, 'YYYY-MM') as cohort_month,
        round(SUM(od.unitPrice * od.quantity * (1 - od.discount)),2) AS total_order_value
    FROM
         customer_dim cd
    LEFT JOIN
       orders o ON cd.customerID = o.customerID
    LEFT JOIN
        order_details od ON o.orderID = od.orderID
    GROUP BY
        cd.country, cohort_month
)
SELECT
    chg.country,
    chg.cohort_month,
    chg.number_of_customers,
    COALESCE(cov.total_order_value, 0) AS total_order_value
FROM
    customer_cohort_grid chg
LEFT JOIN
    cohort_order_value cov
ON
    chg.country = cov.country AND chg.cohort_month = cov.cohort_month
ORDER BY
    chg.country, chg.cohort_month