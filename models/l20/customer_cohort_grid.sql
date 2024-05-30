------ I will create two separated models,
-- first model shows cohort_month to country new customer count

with orders as(
    select * from {{ ref('stg_orders') }}
),
order_details as(
    select * from {{ ref('stg_order_details') }}
),
customers as(
    select * from  {{ ref('stg_customers') }}
),
customer_first_purchase as (
    select customerID, to_char(first_purchase_date,
	'YYYY-MM') as first_purchase_month, country from {{ ref('customer_dim') }}
),

all_months as (
select
	distinct to_char(o.orderDate,
	'YYYY-MM') as month
from
	orders o
),
distinct_countries as(
	select
		distinct country
	from
		customers
),
full_cohort_grid as(
select
	country,
	month
from distinct_countries dc
cross join
        all_months m
 ),
monthly_cohort_base as (
select
	country,
	first_purchase_month,
	COUNT(customerID) as number_of_customers
from
	customer_first_purchase
group by
	country,
	first_purchase_month
),
monthly_cohort_mapped as (
select
	fcg.country,
	fcg.month as cohort_month,
	coalesce(mcb.number_of_customers,
	0) as number_of_customers
from
	full_cohort_grid fcg
left join
        monthly_cohort_base mcb
    on
	fcg.country = mcb.country
	and fcg.month = mcb.first_purchase_month
)
select
	*
from
	monthly_cohort_mapped