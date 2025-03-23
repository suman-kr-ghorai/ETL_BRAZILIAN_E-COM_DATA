from google.cloud import bigquery
import os 

# Load environment variables
PROJECT_ID = os.getenv("PROJECT_ID", "brazilian-ecom")  # Default if not set
DATASET_ID = os.getenv("DATASET_ID", "t6")

# Set up BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Define KPI table schemas
table_schemas = {
    "kpi_orders_summary": [
        bigquery.SchemaField("order_status", "STRING"),
        bigquery.SchemaField("total_orders", "INTEGER"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
        bigquery.SchemaField("avg_order_value", "FLOAT"),
    ],
    "kpi_customer_lifetime_value": [
        bigquery.SchemaField("customer_key", "INTEGER"),
        bigquery.SchemaField("total_spent", "FLOAT"),
        bigquery.SchemaField("total_orders", "INTEGER"),
        bigquery.SchemaField("avg_order_value", "FLOAT"),
    ],
    "kpi_seller_performance": [
        bigquery.SchemaField("seller_key", "INTEGER"),
        bigquery.SchemaField("seller_city", "STRING"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
        bigquery.SchemaField("total_orders", "INTEGER"),
    ],
    "kpi_product_performance": [
        bigquery.SchemaField("product_key", "INTEGER"),
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
        bigquery.SchemaField("total_orders", "INTEGER"),
    ],
    "kpi_yoy_revenue": [
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
    ],
    "kpi_yoy_seller_city": [
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("seller_city", "STRING"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
    ],
    "kpi_yoy_product_category": [
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
    ],
}

# Function to create and populate KPI tables
def create_and_populate_kpi_tables(project_id, dataset_id):
    # Create tables
    for table_name, schema in table_schemas.items():
        table_ref = client.dataset(dataset_id).table(table_name)
        table = bigquery.Table(table_ref, schema=schema)
        try:
            client.create_table(table)
            print(f"KPI Table {table_name} created.")
        except Exception as e:
            if "Already Exists" in str(e):
                print(f"KPI Table {table_name} already exists.")
            else:
                print(f"Error creating KPI Table {table_name}: {e}")

    # Define queries for populating tables
    queries = {
    "kpi_orders_summary": f"""
        DELETE FROM `{project_id}.{dataset_id}.kpi_orders_summary` WHERE TRUE;
        INSERT INTO `{project_id}.{dataset_id}.kpi_orders_summary`
        SELECT 
            order_status, 
            COUNT(order_id) AS total_orders, 
            SUM(payment_value) AS total_revenue, 
            AVG(payment_value) AS avg_order_value
        FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered`
        GROUP BY order_status;
    """,

    "kpi_customer_lifetime_value": f"""
        DELETE FROM `{project_id}.{dataset_id}.kpi_customer_lifetime_value` WHERE TRUE;
        INSERT INTO `{project_id}.{dataset_id}.kpi_customer_lifetime_value`
        SELECT 
            customer_key, 
            SUM(payment_value) AS total_spent, 
            COUNT(order_id) AS total_orders, 
            AVG(payment_value) AS avg_order_value
        FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered`
        GROUP BY customer_key;
    """,

    "kpi_seller_performance": f"""
        DELETE FROM `{project_id}.{dataset_id}.kpi_seller_performance` WHERE TRUE;
        INSERT INTO `{project_id}.{dataset_id}.kpi_seller_performance`
        SELECT 
            o.seller_key, 
            s.seller_city,  
            SUM(o.payment_value) AS total_revenue, 
            COUNT(o.order_id) AS total_orders
        FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` AS o
        JOIN `{project_id}.{dataset_id}.dim_sellers` AS s 
        ON o.seller_key = s.seller_key
        GROUP BY o.seller_key, s.seller_city;
    """,

    "kpi_yoy_revenue": f"""
        DELETE FROM `{project_id}.{dataset_id}.kpi_yoy_revenue` WHERE TRUE;
        INSERT INTO `{project_id}.{dataset_id}.kpi_yoy_revenue`
        SELECT 
            EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
            SUM(payment_value) AS total_revenue
        FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered`
        GROUP BY year;
    """,

    "kpi_yoy_seller_city": f"""
        DELETE FROM `{project_id}.{dataset_id}.kpi_yoy_seller_city` WHERE TRUE;
        INSERT INTO `{project_id}.{dataset_id}.kpi_yoy_seller_city`
        SELECT 
            EXTRACT(YEAR FROM o.order_purchase_timestamp) AS year,
            s.seller_city, 
            SUM(o.payment_value) AS total_revenue
        FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` AS o
        JOIN `{project_id}.{dataset_id}.dim_sellers` AS s 
        ON o.seller_key = s.seller_key
        GROUP BY year, s.seller_city;
    """,

    "kpi_yoy_product_category": f"""
        DELETE FROM `{project_id}.{dataset_id}.kpi_yoy_product_category` WHERE TRUE;
        INSERT INTO `{project_id}.{dataset_id}.kpi_yoy_product_category`
        SELECT 
            EXTRACT(YEAR FROM o.order_purchase_timestamp) AS year,
            p.product_category_name,
            SUM(o.payment_value) AS total_revenue
        FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` AS o
        JOIN `{project_id}.{dataset_id}.dim_products` AS p
        ON o.product_key = p.product_key
        GROUP BY year, p.product_category_name;
    """,

    "kpi_product_performance": f"""
        DELETE FROM `{project_id}.{dataset_id}.kpi_product_performance` WHERE TRUE;
        INSERT INTO `{project_id}.{dataset_id}.kpi_product_performance`
        SELECT 
            p.product_key,
            p.product_category_name,
            SUM(o.payment_value) AS total_revenue,
            COUNT(o.order_id) AS total_orders
        FROM `{project_id}.{dataset_id}.fact_orders_partitioned_clustered` AS o
        JOIN `{project_id}.{dataset_id}.dim_products` AS p
        ON o.product_key = p.product_key
        GROUP BY p.product_key, p.product_category_name;
    """
}

    # Execute queries
    for table_name, query in queries.items():
        try:
            query_job = client.query(query)
            query_job.result()
            print(f"KPI Table {table_name} populated successfully.")
        except Exception as e:
            print(f"Error populating {table_name}: {e}")

# Run the function
# create_and_populate_kpi_tables(PROJECT_ID, DATASET_ID)
