with orders as(
    select * from {{ ref('stg_orders') }}
),
order_details as(
    select * from {{ ref('stg_order_details') }}
),
customers as(
    select * from  {{ ref('stg_customers') }}
),
products as(
    select * from  {{ ref('stg_products') }}
),

customer_purchase_dates AS (
    SELECT
        customerID,
        MIN(orderDate) AS first_purchase_date,
        MAX(orderDate) AS last_purchase_date
    FROM
        orders o
    GROUP BY
        customerID
),
product_sales AS (
    SELECT
        o.orderID,
        o.orderDate,
        o.customerID,
        c.companyName AS customer_name,
        CASE
            WHEN o.orderDate = cpd.first_purchase_date THEN 'New'
            ELSE 'Returning'
        END AS customer_type,
        last_purchase_date - first_purchase_date AS purchase_duration_days,
        od.productID,
        p.productName,
        od.unitPrice,
        od.quantity,
        od.discount,
        (od.unitPrice * od.quantity * (1 - od.discount)) AS total_sales_amount, --- assuming that the discount is shown in pct
        o.shipVia,
        o.freight,
        o.shipName,
        o.shipAddress,
        o.shipCity,
        o.shipRegion,
        o.shipPostalCode,
        o.shipCountry
    FROM
         orders o
    LEFT JOIN
       order_details od ON o.orderID = od.orderID
    LEFT JOIN
        customers c ON o.customerID = c.customerID
    LEFT JOIN
        products p ON od.productID = p.productID
    LEFT JOIN
        customer_purchase_dates cpd ON o.customerID = cpd.customerID
)
SELECT
    orderID,
    orderDate,
    customerID,
    customer_name,
    customer_type,
    purchase_duration_days,
    productID,
    productName,
    unitPrice,
    quantity,
    discount,
    total_sales_amount,
    shipVia,
    freight,
    shipName,
    shipAddress,
    shipCity,
    shipRegion,
    shipPostalCode,
    shipCountry
FROM
    product_sales
ORDER BY
    orderdate desc
