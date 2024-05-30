with orders as(
    select * from {{ ref('stg_orders') }}
),

customer_orders AS (
    SELECT
        customerID,
        min(orderDate) AS min_order_date,
        max(orderDate) AS max_order_date
    FROM
        orders
    GROUP BY
        customerID
)
SELECT
    *,
    max_order_date - min_order_date as first_last_order_days_delta
from
    customer_orders