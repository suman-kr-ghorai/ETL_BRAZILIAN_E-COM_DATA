from google.cloud import bigquery
import streamlit as st



def create_data_marts(client, project_id, dataset_id):
    sales_performance_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.sales_performance_mart` AS
    SELECT
        DATE(order_purchase_timestamp) AS order_date,
        product_category_name_english AS category,
        seller_id,
        SUM(price) AS total_revenue,
        COUNT(order_id) AS total_orders
    FROM `{project_id}.{dataset_id}.fact_orders`
    JOIN `{project_id}.{dataset_id}.dim_products` USING (product_id)
    GROUP BY order_date, category, seller_id;
    """

    product_category_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.product_category_analysis_mart` AS
    SELECT
        product_category_name_english AS category,
        COUNT(DISTINCT product_id) AS total_products,
        SUM(price) AS total_sales,
        AVG(review_score) AS avg_review_score
    FROM `{project_id}.{dataset_id}.fact_orders`
    JOIN `{project_id}.{dataset_id}.dim_products` USING (product_id)
    JOIN `{project_id}.{dataset_id}.dim_reviews` USING (order_id)
    GROUP BY category;
    """

    client.query(sales_performance_query).result()
    client.query(product_category_query).result()
    print("Data Marts Created Successfully!")


