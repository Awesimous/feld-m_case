with orders as(
    select * from {{ ref('stg_orders') }}
),
order_details as(
    select * from {{ ref('stg_order_details') }}
),
customers as(
    select * from  {{ ref('stg_customers') }}
),


customer_first_last_purchase_date as (
select
	customerID,
    MIN(orderDate) as first_purchase_date,
	MAX(orderDate) as last_purchase_date
from
	orders
group by
	customerID
),
prev_orders as (
select
	*,
	lag(orderDate) over (partition by customerID
order by
	orderDate) as prev_orderDate
from
	orders
),
customer_orders as (
select
	po.customerID,
	po.orderID,
	po.orderDate,
	po.prev_orderDate,
	SUM(od.unitPrice * od.quantity * (1 - od.discount)) as total_order_value
from
	prev_orders po
left join
        order_details od on
	po.orderID = od.orderID
group by
	1,
	2,
	3,
	4
),
date_customer_orders as(
select
	customerID,
	orderID,
	orderDate,
	total_order_value,
	orderDate - prev_orderDate as order_date_diff
from
	customer_orders
),
customer_metrics as (
select
	customerID,
	COUNT(orderID) as number_of_orders,
	MAX(total_order_value) as most_expensive_order,
	SUM(total_order_value) as total_revenue,
	round(AVG(order_date_diff)) as avg_days_between_orders
from
	date_customer_orders
group by
	1
),
top_customers as (
select
	customerID,
	total_revenue,
	row_number() over (
order by
	total_revenue desc) as customer_rank
from
	customer_metrics
),
top_10_customers as (
select
	customerID
from
	top_customers
where
	customer_rank <= 10
)
select
	c.customerID,
	c.companyName,
	c.contactName,
	c.contactTitle,
	c.address,
	c.city,
	c.region,
	c.postalCode,
	c.country,
	c.phone,
	c.fax,
	cm.number_of_orders,
	cm.most_expensive_order,
	cm.avg_days_between_orders,
    cpd.first_purchase_date,
	cpd.last_purchase_date,
	current_date - cpd.last_purchase_date as days_since_last_order, -- since its a very old dataset it doesnt really show any cool insights
	case
		when tc.customerID is not null then 'Yes'
		else 'No'
	end as top_10_customer
from
	customers c
join
    customer_metrics cm on
	c.customerID = cm.customerID
left join
    top_10_customers tc on
	c.customerID = tc.customerID
left join customer_first_last_purchase_date cpd on
	cpd.customerID = c.customerID
order by
	c.customerID