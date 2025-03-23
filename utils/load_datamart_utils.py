import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    filename="datamart_creation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def create_datamart_tables(project_id, dataset_id):
    """Creates aggregated datamart tables in BigQuery."""
    client = bigquery.Client(project=project_id)
    
    queries = {
        "dm_sales_by_category": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dm_sales_by_category` AS
            SELECT 
                dp.product_category_name_english AS category, 
                SUM(fo.price) AS total_sales,
                SUM(fo.freight_value) AS total_freight,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            JOIN `{project_id}.{dataset_id}.dim_products` dp
            ON fo.product_key = dp.product_key
            GROUP BY category;
        """,
        "dm_sales_by_customer_state": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dm_sales_by_customer_state` AS
            SELECT 
                dc.customer_state, 
                SUM(fo.price) AS total_sales,
                COUNT(DISTINCT fo.customer_key) AS unique_customers,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            JOIN `{project_id}.{dataset_id}.dim_customers` dc
            ON fo.customer_key = dc.customer_key
            GROUP BY dc.customer_state;
        """,
        "dm_sales_by_seller": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dm_sales_by_seller` AS
            SELECT 
                ds.seller_id, 
                ds.seller_state,
                SUM(fo.price) AS total_sales,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            JOIN `{project_id}.{dataset_id}.dim_sellers` ds
            ON fo.seller_key = ds.seller_key
            GROUP BY ds.seller_id, ds.seller_state;
        """,
        "dm_monthly_sales_trend": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dm_monthly_sales_trend` AS
            SELECT 
                FORMAT_TIMESTAMP('%Y-%m', fo.order_purchase_timestamp) AS month,
                SUM(fo.price) AS total_sales,
                COUNT(fo.order_id) AS total_orders
            FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` fo
            GROUP BY month
            ORDER BY month;
        """,
        "dm_avg_review_score": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dm_avg_review_score` AS
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
            GROUP BY dp.product_key, category;
        """
    }
    
    for table_name, query in queries.items():
        try:
            query_job = client.query(query)
            query_job.result()
            logger.info(f"✅ Created {table_name} successfully.")
            print(f"✅ Created {table_name} successfully.")
        except Exception as e:
            logger.error(f"❌ Error creating {table_name}: {e}")
            print(f"❌ Error creating {table_name}: {e}")
