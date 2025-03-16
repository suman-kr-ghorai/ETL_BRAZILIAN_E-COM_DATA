import streamlit as st
import logging
import time
import os
from google.cloud import bigquery
from utils.kaggle_utils import fetch_data
from utils.mysql_fetch_utils import fetch_table
from utils.convert_mysql_dtypes import convert_mysql_dtypes 
from utils.merge_df import merge_ecommerce_data
from utils.cleaner_utils import clean
from utils.convert_dtypes import convert_dtypes
from utils.bigquery_upload_utils import upload_to_bigquery
from utils.schema_utils import generate_star_schema
import pandas as pd




def main():
    st.title("ETL-P")
    
    st.write("Fetching data from Kaggle...")
    fetch_data()
    time.sleep(5)
    st.success("Data fetched successfully!")
    
    st.write("Fetching tables from MySQL and converting data types...")
    try:
        orders = convert_mysql_dtypes(fetch_table("olist_orders_dataset"))
        products = convert_mysql_dtypes(fetch_table("olist_products_dataset"))
        sellers = convert_mysql_dtypes(fetch_table("olist_sellers_dataset"))
        category_translation = convert_mysql_dtypes(fetch_table("product_category_name_translation"))
        customers = pd.read_csv(r"data/olist_customers_dataset.csv")
        order_items = pd.read_csv(r"data/olist_order_items_dataset.csv")
        order_payments = pd.read_csv(r"data/olist_order_payments_dataset.csv")
        order_reviews = pd.read_csv(r"data/olist_order_reviews_dataset.csv")
    except Exception as e:
        st.error(f"Error fetching data from MySQL: {e}")
        logging.error(f"Error fetching data from MySQL: {e}")
        return
    time.sleep(5)
    st.success("Data fetched and converted successfully!")
    
    st.write("Merging data...")
    merged_df = merge_ecommerce_data(orders, customers, order_payments, order_reviews, order_items, products, category_translation, sellers)
    time.sleep(5)
    st.success("Data merged successfully!")
    
    st.write("Cleaning data...")
    cleaned_df = clean(merged_df)
    time.sleep(5)
    st.success("Data cleaned successfully!")
    
    st.write("Converting data types...")
    converted_df = convert_dtypes(cleaned_df)
    time.sleep(5)
    st.success("Data types converted successfully!")
    
    st.write("Generating star schema tables...")
    dim_customers, dim_sellers, dim_products, dim_payment_types, dim_reviews, fact_orders = generate_star_schema(converted_df)
    time.sleep(5)
    st.success("Star schema tables generated successfully!")
    
    st.write("### Customers Table")
    st.write(dim_customers.head())
    
    st.write("### Sellers Table")
    st.write(dim_sellers.head())
    
    st.write("### Products Table")
    st.write(dim_products.head())
    
    st.write("### Payment Types Table")
    st.write(dim_payment_types.head())
    
    st.write("### Reviews Table")
    st.write(dim_reviews.head())
    
    st.write("### Orders Fact Table")
    st.write(fact_orders.head())
    
    project_id = st.text_input("Enter Google Cloud Project ID:")
    dataset_id = st.text_input("Enter BigQuery Dataset ID:")
    
    if st.button("Upload Data to BigQuery"):
        if project_id and dataset_id:
            # Upload fact_orders with partitioning on 'order_purchase_timestamp'
            upload_to_bigquery(fact_orders, "fact_orders", project_id, dataset_id, partition_col="order_purchase_timestamp")
        
            # Upload other dimension tables without partitioning
            upload_to_bigquery(dim_customers, "dim_customers", project_id, dataset_id)
            upload_to_bigquery(dim_sellers, "dim_sellers", project_id, dataset_id)
            upload_to_bigquery(dim_products, "dim_products", project_id, dataset_id)
            upload_to_bigquery(dim_payment_types, "dim_payment_types", project_id, dataset_id)
            upload_to_bigquery(dim_reviews, "dim_reviews", project_id, dataset_id)
        
            st.success("All tables uploaded successfully!")
        else:
            st.error("Please provide valid Project ID and Dataset ID.")


if __name__ == "__main__":
    main()
