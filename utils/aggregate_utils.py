from google.cloud import bigquery
import logging
from google.cloud import bigquery
import streamlit as st

def create_aggregation_tables(project_id, dataset_id):
    client = bigquery.Client(project=project_id)

    queries = {
        "Agg_sales_by_category": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_sales_by_category` AS
            SELECT 
                product_category_name_english AS category, 
                SUM(price) AS total_sales,
                SUM(freight_value) AS total_freight,
                COUNT(order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders` fo
            JOIN `{project_id}.{dataset_id}.dim_products` dp
            ON fo.product_id = dp.product_id
            GROUP BY category
        """,
        
        "Agg_sales_by_customer_state": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_sales_by_customer_state` AS
            SELECT 
                dc.customer_state, 
                SUM(price) AS total_sales,
                COUNT(DISTINCT fo.customer_id) AS unique_customers,
                COUNT(order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders` fo
            JOIN `{project_id}.{dataset_id}.dim_customers` dc
            ON fo.customer_id = dc.customer_id
            GROUP BY dc.customer_state
        """,
        
        "Agg_sales_by_seller": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_sales_by_seller` AS
            SELECT 
                ds.seller_id, 
                ds.seller_state,
                SUM(price) AS total_sales,
                COUNT(order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders` fo
            JOIN `{project_id}.{dataset_id}.dim_sellers` ds
            ON fo.seller_id = ds.seller_id
            GROUP BY ds.seller_id, ds.seller_state
        """,
        
        "Agg_monthly_sales_trend": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_monthly_sales_trend` AS
            SELECT 
                FORMAT_DATE('%Y-%m', order_purchase_timestamp) AS month,
                SUM(price) AS total_sales,
                COUNT(order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders`
            GROUP BY month
            ORDER BY month
        """,
        
        "Agg_avg_review_score": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_avg_review_score` AS
            SELECT 
                dp.product_id,
                dp.product_category_name_english AS category,
                AVG(dr.review_score) AS avg_review_score,
                COUNT(dr.review_id) AS total_reviews
            FROM `{project_id}.{dataset_id}.fact_orders` fo
            JOIN `{project_id}.{dataset_id}.dim_products` dp
            ON fo.product_id = dp.product_id
            JOIN `{project_id}.{dataset_id}.dim_reviews` dr
            ON fo.order_id = dr.order_id
            GROUP BY dp.product_id, category
        """
    }

    for table_name, query in queries.items():
        print(f"Creating aggregation table: {table_name}...")
        query_job = client.query(query)
        query_job.result()  # Wait for query execution
        print(f"Table `{table_name}` created successfully!")

    print("All aggregation tables have been created.")


from google.cloud import bigquery

def get_aggregation_tables(project_id, dataset_id):
    """
    Fetches all aggregation tables (tables starting with 'Agg_') in the given dataset.
    """
    client = bigquery.Client(project=project_id)
    
    query = f"""
        SELECT table_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'Agg_%'
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        return [row.table_name for row in results]
    
    except Exception as e:
        print(f"Error fetching aggregation tables: {e}")
        return []


def fetch_aggregation_table(project_id, dataset_id, table_name):
    try:
        client = bigquery.Client(project=project_id)  # Explicitly pass project_id
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Error fetching {table_name}: {e}")
        st.error(f"Error fetching {table_name}: {e}")
        return None


