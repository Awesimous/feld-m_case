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


product_sales AS (
    SELECT
        p.productID,
        p.productName,
        c.customerID,
        c.companyName AS customer_name,
        o.orderID,
        o.orderDate,
        od.unitPrice,
        od.quantity,
        od.discount,
        -- Calculate total sales amount for each order line
        (od.unitPrice * od.quantity * (1 - od.discount)) AS total_sales_amount
    FROM
        orders o
    LEFT JOIN
        order_details od ON o.orderID = od.orderID
    LEFT JOIN
        customers c ON o.customerID = c.customerID
    LEFT JOIN
        products p ON od.productID = p.productID
)

SELECT
    productID,
    productName,
    COUNT(DISTINCT orderID) AS num_orders,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_sales_amount) AS total_sales_amount,
    AVG(unitPrice) AS average_unit_price,
    AVG(discount) AS average_discount,
    MIN(orderDate) AS first_order_date,
    MAX(orderDate) AS last_order_date
FROM
    product_sales
GROUP BY
    productID, productName
