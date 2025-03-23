import streamlit as st
import logging
import time
import pandas as pd
import numpy as np
from google.cloud import bigquery
from utils.kaggle_utils import fetch_data
from utils.mysql_fetch_utils import fetch_table
from utils.convert_mysql_dtypes import convert_mysql_dtypes 
from utils.merge_df import merge_ecommerce_data
from utils.cleaner_utils import clean
from utils.convert_dtypes import convert_dtypes
from utils.bigquery_upload_utils import upload_fact_table, upload_dimension_table
from utils.schema_utils import generate_star_schema
from utils.aggregate_utils import create_aggregation_tables
from utils.load_datamart_utils import create_datamart_tables
from config import PROJECT_ID, DATASET_ID

st.set_page_config(page_title="ETL Dashboard", layout="wide")

st.sidebar.title("ETL Dashboard")
page = st.sidebar.radio(" /", [
    "ETL - PIPELINE MANUAL",
    "ETL - AUTOMATED",
   
])

def show_loader(message, duration=3):
    """Displays a loading animation while executing tasks."""
    with st.status(message, expanded=True) as status:
        time.sleep(duration)
        status.update(label=f"{message} Completed!", state="complete")

def display_data_insights(df, title="Data Insights"):
    """Displays shape, total records, and total null values per column in a DataFrame."""
    st.subheader(title)
    st.write(f"**Shape:** {df.shape}")
    st.write(f"**Total Records:** {len(df)}")
    
    # Display total null values for each column
    null_counts = df.isna().sum().sum()
    # null_summary = pd.DataFrame({'Column': null_counts.index, 'Total Null Values': null_counts.values})
    
    st.write(f"**Total Null Values:**{null_counts}")
    # st.dataframe(null_summary)


if page == "ETL - PIPELINE MANUAL":
    st.title("ETL - PIPELINE MANUAL")
    
    if st.button("Start Extraction"):
        show_loader("Fetching data from Kaggle...")
        # fetch_data()

        show_loader("Fetching MySQL tables...")
        st.session_state.orders = convert_mysql_dtypes(fetch_table("olist_orders_dataset"))
        st.session_state.products = convert_mysql_dtypes(fetch_table("olist_products_dataset"))
        st.session_state.sellers = convert_mysql_dtypes(fetch_table("olist_sellers_dataset"))
        st.session_state.category_translation = convert_mysql_dtypes(fetch_table("product_category_name_translation"))
        st.session_state.customers = pd.read_csv(r"data/olist_customers_dataset.csv")
        st.session_state.order_items = pd.read_csv(r"data/olist_order_items_dataset.csv")
        st.session_state.order_payments = pd.read_csv(r"data/olist_order_payments_dataset.csv")
        st.session_state.order_reviews = pd.read_csv(r"data/olist_order_reviews_dataset.csv")
        
        st.success("Data fetched successfully!")
    
    if "orders" in st.session_state:
        if st.button("View Raw Data Insights"):
            raw_data = merge_ecommerce_data(st.session_state.orders, st.session_state.customers, 
                                            st.session_state.order_payments, st.session_state.order_reviews, 
                                            st.session_state.order_items, st.session_state.products, 
                                            st.session_state.category_translation, st.session_state.sellers)
            display_data_insights(raw_data, "Insights Before Transformation")

    if st.button("Do Transformation & Cleaning"):
        show_loader("Merging and Cleaning Data...")
        merged_df = merge_ecommerce_data(st.session_state.orders, st.session_state.customers, 
                                         st.session_state.order_payments, st.session_state.order_reviews, 
                                         st.session_state.order_items, st.session_state.products, 
                                         st.session_state.category_translation, st.session_state.sellers)
        
        cleaned_df = clean(merged_df)
        st.session_state.converted_df = convert_dtypes(cleaned_df)
        st.success("Data transformed successfully!")

        display_data_insights(st.session_state.converted_df, "Insights After Transformation")
    
    if "converted_df" in st.session_state:
        if st.button("Generate Star Schema"):
            show_loader("Generating Star Schema...")
            st.session_state.dim_customers, st.session_state.dim_sellers, st.session_state.dim_products, \
            st.session_state.dim_payment_types, st.session_state.dim_reviews, st.session_state.fact_orders = generate_star_schema(st.session_state.converted_df)
            st.success("Star schema tables generated successfully!")

    if "fact_orders" in st.session_state:
        if st.button("Upload to BigQuery & Generate Data Marts"):
            show_loader("Uploading Tables to BigQuery...")
            for table_name, table_data in {
                "dim_customers": st.session_state.dim_customers,
                "dim_sellers": st.session_state.dim_sellers,
                "dim_products": st.session_state.dim_products,
                "dim_payment_types": st.session_state.dim_payment_types,
                "dim_reviews": st.session_state.dim_reviews
            }.items():
                upload_dimension_table(table_data, table_name, PROJECT_ID, DATASET_ID)

            upload_fact_table(st.session_state.fact_orders, PROJECT_ID, DATASET_ID)

            show_loader("Creating Data Marts...")
            create_datamart_tables(PROJECT_ID, DATASET_ID)
            st.success("Data marts created successfully!")

    if st.button("Create Aggregation Tables"):
        show_loader("Creating Aggregation Tables...")
        create_aggregation_tables(PROJECT_ID, DATASET_ID)
        st.success("Aggregation tables created successfully!")

elif page == "ETL - AUTOMATED":
    st.title("ETL - AUTOMATED")
    if st.button("Run ETL Pipeline Automatically"):
        show_loader("Running Full ETL Pipeline...")
        fetch_data()
        orders = convert_mysql_dtypes(fetch_table("olist_orders_dataset"))
        products = convert_mysql_dtypes(fetch_table("olist_products_dataset"))
        sellers = convert_mysql_dtypes(fetch_table("olist_sellers_dataset"))
        category_translation = convert_mysql_dtypes(fetch_table("product_category_name_translation"))
        customers = pd.read_csv(r"data/olist_customers_dataset.csv")
        order_items = pd.read_csv(r"data/olist_order_items_dataset.csv")
        order_payments = pd.read_csv(r"data/olist_order_payments_dataset.csv")
        order_reviews = pd.read_csv(r"data/olist_order_reviews_dataset.csv")

        show_loader("Merging and Cleaning Data...")
        merged_df = merge_ecommerce_data(orders, customers, order_payments, order_reviews, order_items, products, category_translation, sellers)
        cleaned_df = clean(merged_df)
        converted_df = convert_dtypes(cleaned_df)

        display_data_insights(converted_df, "Insights After Transformation")

        show_loader("Generating Star Schema...")
        dim_customers, dim_sellers, dim_products, dim_payment_types, dim_reviews, fact_orders = generate_star_schema(converted_df)

        show_loader("Uploading Tables to BigQuery...")
        for table_name, table_data in {
            "dim_customers": dim_customers,
            "dim_sellers": dim_sellers,
            "dim_products": dim_products,
            "dim_payment_types": dim_payment_types,
            "dim_reviews": dim_reviews
        }.items():
            upload_dimension_table(table_data, table_name, PROJECT_ID, DATASET_ID)
        
        upload_fact_table(fact_orders, PROJECT_ID, DATASET_ID)

        show_loader("Creating Aggregation Tables...")
        create_aggregation_tables(PROJECT_ID, DATASET_ID)

        show_loader("Creating Data Marts...")
        create_datamart_tables(PROJECT_ID, DATASET_ID)

        st.success("ETL Pipeline Completed Successfully!")

elif page == "Analysis & Visualization":
    st.title("Analysis & Visualization")
    st.write("Data visualizations will be implemented here.")

elif page == "Conclusion":
    st.title("Conclusion")
    st.write("Final insights and summary of the ETL process.")
