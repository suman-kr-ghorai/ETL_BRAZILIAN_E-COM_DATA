import os
import logging
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
# env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))

# load_dotenv(env_path)

# BigQuery Config
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

def get_schemas():
    """Returns a dictionary of table schemas for dimensions and fact tables."""
    return {
        "dim_customers": [
            bigquery.SchemaField("customer_key", "INTEGER"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("customer_unique_id", "STRING"),
            bigquery.SchemaField("customer_zip_code_prefix", "INTEGER"),
            bigquery.SchemaField("customer_city", "STRING"),
            bigquery.SchemaField("customer_state", "STRING")
        ],
        "dim_sellers": [
            bigquery.SchemaField("seller_key", "INTEGER"),
            bigquery.SchemaField("seller_id", "STRING"),
            bigquery.SchemaField("seller_zip_code_prefix", "INTEGER"),
            bigquery.SchemaField("seller_city", "STRING"),
            bigquery.SchemaField("seller_state", "STRING")
        ],
        "dim_products": [
            bigquery.SchemaField("product_key", "INTEGER"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("product_category_name", "STRING"),
            bigquery.SchemaField("product_category_name_english", "STRING"),
            bigquery.SchemaField("product_name_length", "INTEGER"),
            bigquery.SchemaField("product_description_length", "INTEGER"),
            bigquery.SchemaField("product_photos_qty", "INTEGER"),
            bigquery.SchemaField("product_weight_g", "FLOAT"),
            bigquery.SchemaField("product_length_cm", "FLOAT"),
            bigquery.SchemaField("product_height_cm", "FLOAT"),
            bigquery.SchemaField("product_width_cm", "FLOAT")
        ],
        "dim_payment_types": [
            bigquery.SchemaField("payment_type_key", "INTEGER"),
            bigquery.SchemaField("payment_type", "STRING")
        ],
        "dim_reviews": [
            bigquery.SchemaField("review_key", "INTEGER"),
            bigquery.SchemaField("review_id", "STRING"),
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("review_score", "INTEGER")
        ],
        "fact_orders": [
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("customer_key", "INTEGER"),
            bigquery.SchemaField("seller_key", "INTEGER"),
            bigquery.SchemaField("product_key", "INTEGER"),
            bigquery.SchemaField("payment_type_key", "INTEGER"),
            bigquery.SchemaField("order_status", "STRING"),
            bigquery.SchemaField("order_purchase_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("order_approved_at", "TIMESTAMP"),
            bigquery.SchemaField("order_delivered_carrier_date", "TIMESTAMP"),
            bigquery.SchemaField("order_delivered_customer_date", "TIMESTAMP"),
            bigquery.SchemaField("order_estimated_delivery_date", "TIMESTAMP"),
            bigquery.SchemaField("shipping_limit_date", "TIMESTAMP"),
            bigquery.SchemaField("payment_installments", "INTEGER"),
            bigquery.SchemaField("payment_value", "FLOAT"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("freight_value", "FLOAT")
        ]
    }

def upload_dimension_table(df, table_name,PROJECT_ID,DATASET_ID):
    """Uploads a dimension table to BigQuery."""
    schemas = get_schemas()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schemas[table_name]
    )

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"✅ Uploaded {table_name} successfully.")
        print(f"✅ Uploaded {table_name} successfully.")
    except Exception as e:
        logger.error(f"❌ Error uploading {table_name}: {e}")
        print(f"❌ Error uploading {table_name}: {e}")

def upload_fact_table(df,PROJECT_ID,DATASET_ID):
    """Uploads the fact_orders table with partitioning & clustering."""
    schemas = get_schemas()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.fact_orders_partitioned_clustered"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schemas["fact_orders"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="order_purchase_timestamp"
        ),
        clustering_fields=["customer_key", "seller_key", "product_key"]
    )

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info("✅ Uploaded fact_orders_partitioned_clustered successfully.")
        print("✅ Uploaded fact_orders_partitioned_clustered successfully.")
    except Exception as e:
        logger.error(f"❌ Error uploading fact_orders: {e}")
        print(f"❌ Error uploading fact_orders: {e}")

