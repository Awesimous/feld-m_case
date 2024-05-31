# Feld-M DBT Project for Sales Data Analysis

## Project Overview

This project leverages dbt (data build tool) to transform raw sales data into a structured and analyzable format. I created multiple models to generate insights into sales transactions, customer behaviors, and cohort analysis. The following tasks were completed:

1. **Transactional Fact Table for Sales**: Created a fact table with the grain set at the product level.
2. **Customer Dimension Table**: Developed a dimension table for customers with additional metrics.
3. **Monthly Cohorts Dimension Table**: Built a dimension table for monthly cohorts at the country level.
4. **Product Dimension Table**: Developed a dimension table for products with additional metrics. (additional)

## Tools Used

- **dbt**: For data transformations and creating models.
- **PostgreSQL**: As the database for executing SQL queries.
- **dbeaver**: Database administration
- **Git**: For version control.
- **GitHub**: For repository management.
- **chatgpt**: Support with db commands, readme file markdown

## Task Details and Thought Process

### i. Transactional Fact Table for Sales

**Objective**: Create a transactional fact table with a product-level grain and additional dimensions and metrics: new or returning customer and the number of days between first and last purchase.

**Steps**:
1. **Calculate Metrics**: new / returning customer and number of days between first and last purchase
2. **Create Fact Table Sales**: Combine the calculated metrics.

### ii. Customer Dimension Table

**Objective**: Create a dimension table for customers with additional metrics: number of orders, value of the most expensive order, top 10 customers by revenue, average number of days between orders, and days since the most recent order.

**Steps**:
1. **Calculate Metrics**: Number of orders, total revenue, most expensive order, average days between orders, days since last order.
2. **Identify Top 10 Customers**: By revenue.
3. **Create Customer Dimension Table**: Combine the calculated metrics.

### iii. Monthly Cohorts Dimension Table

**Objective**: Create a dimension table for monthly cohorts at the country level with additional dimensions and metrics: number of customers in the cohort and cohort's total order value.

**Steps**:
1. **Determine First Order Date**: Identify the first order date for each customer.
2. **Assign Customers to Monthly Cohorts**: Based on the first order date.
3. **Calculate Cohort Metrics**: Number of customers and total order value.
4. **Create Cohort Dimension Table**: Combine the calculated metrics into the cohort dimension table, ensuring every cohort is represented.

### iv. Additional task: 
1. Create models for recurring entities like orderdate-related data points
2. Added extra analysis to compare avg time between orders with delta current_date and last_purchase_date, intending to check what customers are past-due to restock their consumables (see churn analysis below)


### Potential Further Analysis:

- **Customer Lifetime Value (CLV)**: Extend the customer dimension table to include metrics for calculating CLV.
- **Churn Analysis**: Identify customers who have stopped making purchases and analyze potential reasons.
- **Sales Forecasting**: Use time series analysis on the sales data to predict future sales trends.

### Challenges:
- **Issues with sqlite and dbt**: Could not get sqlite to work in combination with dbt (maybe due to new dbt version?!)
- **Load csv files into postgres db**: Format of csv file, Null values to be replaced
