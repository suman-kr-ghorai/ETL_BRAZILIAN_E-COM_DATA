import pandas as pd
from google.cloud import bigquery

def push_to_bigquery(df, table_id, project_id):
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Data pushed to {table_id}")

def create_and_push_datamarts(df, project_id, dataset_id):
    # Sales performance per customer
    dm_sales_performance = df.groupby("customer_id").agg(
        total_revenue=pd.NamedAgg(column="payment_value", aggfunc="sum"),
        total_orders=pd.NamedAgg(column="order_id", aggfunc="nunique"),
        avg_order_value=pd.NamedAgg(column="payment_value", aggfunc="mean")
    ).reset_index()
    
    # Product category analysis
    dm_product_category_analysis = df.groupby("product_category_name", observed=False).agg(
        total_sales=pd.NamedAgg(column="order_id", aggfunc="count"),
        total_revenue=pd.NamedAgg(column="payment_value", aggfunc="sum"),
        avg_price=pd.NamedAgg(column="price", aggfunc="mean")
    ).reset_index()
    
    # Customer behavior analysis
    dm_customer_behavior = df.groupby("customer_id").agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc="nunique"),
        distinct_products=pd.NamedAgg(column="product_id", aggfunc="nunique"),
        total_spent=pd.NamedAgg(column="payment_value", aggfunc="sum")
    ).reset_index()
    
    # Filter out undelivered orders
    delivered_orders = df[df["order_delivered_customer_date"].notna()].copy()

    # ✅ Convert to datetime before performing timedelta operations
    delivered_orders["order_delivered_customer_date"] = pd.to_datetime(delivered_orders["order_delivered_customer_date"], errors="coerce")
    delivered_orders["order_purchase_timestamp"] = pd.to_datetime(delivered_orders["order_purchase_timestamp"], errors="coerce")

    # ✅ Compute shipping time in seconds
    delivered_orders["shipping_time"] = (
        delivered_orders["order_delivered_customer_date"] - delivered_orders["order_purchase_timestamp"]
    ).dt.total_seconds()

    # ✅ Handle NaNs and convert to integer
    delivered_orders["shipping_time"] = delivered_orders["shipping_time"].fillna(0).astype(int)

    # Order fulfillment analysis
    dm_order_fulfillment = delivered_orders.groupby("order_status", observed=False).agg(
        avg_shipping_time=pd.NamedAgg(column="shipping_time", aggfunc="mean"),
        total_orders=pd.NamedAgg(column="order_id", aggfunc="count")
    ).reset_index()

    # ✅ Convert avg_shipping_time to integer for consistency
    dm_order_fulfillment["avg_shipping_time"] = dm_order_fulfillment["avg_shipping_time"].fillna(0).astype(int)

    # Payment method analysis
    dm_payment_analysis = df.groupby("payment_type", observed=False).agg(
        total_revenue=pd.NamedAgg(column="payment_value", aggfunc="sum"),
        total_transactions=pd.NamedAgg(column="order_id", aggfunc="count")
    ).reset_index()
    
    # Dictionary of data marts
    data_marts = {
        "dm_sales_performance": dm_sales_performance,
        "dm_product_category_analysis": dm_product_category_analysis,
        "dm_customer_behavior": dm_customer_behavior,
        "dm_order_fulfillment": dm_order_fulfillment,
        "dm_payment_analysis": dm_payment_analysis
    }
    
    # Push each data mart to BigQuery
    for table_name, table_df in data_marts.items():
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        push_to_bigquery(table_df, table_id, project_id)

def run_datamart_pipeline(df, project_id, dataset_id):
    print("Starting Data Mart creation process...")
    create_and_push_datamarts(df, project_id, dataset_id)
    print("Data Marts successfully created and pushed to BigQuery.")
