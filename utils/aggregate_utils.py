import os
import logging
import pandas as pd
from google.cloud import bigquery
import streamlit as st

# Load environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Set up logging
log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app.log"))
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def create_aggregation_tables(project_id, dataset_id):
    """Creates aggregated tables in BigQuery."""
    queries = {
        "Agg_sales_by_category": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_sales_by_category` AS
            SELECT 
                dp.product_category_name_english AS category, 
                SUM(fo.price) AS total_sales,
                SUM(fo.freight_value) AS total_freight,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            JOIN `{project_id}.{dataset_id}.dim_products` dp
            ON fo.product_key = dp.product_key
            GROUP BY category
        """,
        "Agg_sales_by_customer_state": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_sales_by_customer_state` AS
            SELECT 
                dc.customer_state, 
                SUM(fo.price) AS total_sales,
                COUNT(DISTINCT fo.customer_key) AS unique_customers,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            JOIN `{project_id}.{dataset_id}.dim_customers` dc
            ON fo.customer_key = dc.customer_key
            GROUP BY dc.customer_state
        """,
        "Agg_sales_by_seller": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_sales_by_seller` AS
            SELECT 
                ds.seller_id, 
                ds.seller_state,
                SUM(fo.price) AS total_sales,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            JOIN `{project_id}.{dataset_id}.dim_sellers` ds
            ON fo.seller_key = ds.seller_key
            GROUP BY ds.seller_id, ds.seller_state
        """,
        "Agg_monthly_sales_trend": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_monthly_sales_trend` AS
            SELECT 
                FORMAT_DATE('%Y-%m', fo.order_purchase_timestamp) AS month,
                SUM(fo.price) AS total_sales,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            GROUP BY month
            ORDER BY month
        """,
        "Agg_avg_review_score": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.Agg_avg_review_score` AS
            SELECT 
                dp.product_key,
                dp.product_category_name_english AS category,
                AVG(dr.review_score) AS avg_review_score,
                COUNT(dr.review_id) AS total_reviews
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            JOIN `{project_id}.{dataset_id}.dim_products` dp
            ON fo.product_key = dp.product_key
            JOIN `{project_id}.{dataset_id}.dim_reviews` dr
            ON fo.order_id = dr.order_id
            GROUP BY dp.product_key, category
        """
    }
    
    for table_name, query in queries.items():
        try:
            query_job = client.query(query)
            query_job.result()
            logger.info(f"✅ Created {table_name} successfully.")
        except Exception as e:
            logger.error(f"❌ Error creating {table_name}: {e}")
            print(f"❌ Error creating {table_name}: {e}")

def get_aggregation_tables(project_id, dataset_id):
    """Fetches all aggregation tables in the dataset."""
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
        logger.error(f"❌ Error fetching aggregation tables: {e}")
        return []

def fetch_aggregation_table(project_id, dataset_id, table_name):
    """Fetches data from a specified aggregation table."""
    try:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logger.error(f"❌ Error fetching {table_name}: {e}")
        return None
