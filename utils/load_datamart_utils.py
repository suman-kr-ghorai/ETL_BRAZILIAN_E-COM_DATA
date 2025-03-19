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
    dm_sales_performance = df.groupby("customer_id").agg(
        total_revenue=pd.NamedAgg(column="payment_value", aggfunc="sum"),
        total_orders=pd.NamedAgg(column="order_id", aggfunc="nunique"),
        avg_order_value=pd.NamedAgg(column="payment_value", aggfunc="mean")
    ).reset_index()
    
    dm_product_category_analysis = df.groupby("product_category_name", observed=False).agg(
        total_sales=pd.NamedAgg(column="order_id", aggfunc="count"),
        total_revenue=pd.NamedAgg(column="payment_value", aggfunc="sum"),
        avg_price=pd.NamedAgg(column="price", aggfunc="mean")
    ).reset_index()
    
    dm_customer_behavior = df.groupby("customer_id").agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc="nunique"),
        distinct_products=pd.NamedAgg(column="product_id", aggfunc="nunique"),
        total_spent=pd.NamedAgg(column="payment_value", aggfunc="sum")
    ).reset_index()
    
    dm_order_fulfillment = df.groupby("order_status", observed=False).agg(
        avg_shipping_time=pd.NamedAgg(column="order_delivered_customer_date", 
            aggfunc=lambda x: (x - df["order_purchase_timestamp"]).mean()),
        total_orders=pd.NamedAgg(column="order_id", aggfunc="count")
    ).reset_index()

    # ✅ FIX: Convert timedelta to seconds and handle NaNs before pushing to BigQuery
    if "avg_shipping_time" in dm_order_fulfillment.columns:
        dm_order_fulfillment["avg_shipping_time"] = (
            dm_order_fulfillment["avg_shipping_time"]
            .dt.total_seconds()
            .fillna(0)  # Replace NaN values with 0
            .astype("int64")  # Ensure integer conversion
        )
    
    dm_payment_analysis = df.groupby("payment_type", observed=False).agg(
        total_revenue=pd.NamedAgg(column="payment_value", aggfunc="sum"),
        total_transactions=pd.NamedAgg(column="order_id", aggfunc="count")
    ).reset_index()
    
    data_marts = {
        "dm_sales_performance": dm_sales_performance,
        "dm_product_category_analysis": dm_product_category_analysis,
        "dm_customer_behavior": dm_customer_behavior,
        "dm_order_fulfillment": dm_order_fulfillment,
        "dm_payment_analysis": dm_payment_analysis
    }
    
    for table_name, table_df in data_marts.items():
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        push_to_bigquery(table_df, table_id, project_id)

def run_datamart_pipeline(df, project_id, dataset_id):
    print("Starting Data Mart creation process...")
    create_and_push_datamarts(df, project_id, dataset_id)
    print("Data Marts successfully created and pushed to BigQuery.")
