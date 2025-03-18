import streamlit as st
import logging
import time
import pandas as pd
from utils.kaggle_utils import fetch_data
from utils.mysql_fetch_utils import fetch_table
from utils.convert_mysql_dtypes import convert_mysql_dtypes 
from utils.merge_df import merge_ecommerce_data
from utils.cleaner_utils import clean
from utils.convert_dtypes import convert_dtypes
from utils.bigquery_upload_utils import upload_to_bigquery
from utils.schema_utils import generate_star_schema

st.set_page_config(page_title="ETL Dashboard", layout="wide")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Home", "Star Schema", "Upload to BigQuery"])

def show_loader(message):
    with st.spinner(message):
        time.sleep(1)

# Initialize session state if not exists
if 'converted_df' not in st.session_state:
    st.session_state.converted_df = None
if 'star_schema_generated' not in st.session_state:
    st.session_state.star_schema_generated = False

def main():
    if page == "Home":
        st.title("ETL Pipeline Dashboard")
        
        if st.button("Start Extraction"):
            show_loader("Fetching data from Kaggle...")
            # fetch_data()
            st.success("Data fetched successfully!")
            
            show_loader("Fetching tables from MySQL and converting data types...")
            try:
                orders = convert_mysql_dtypes(fetch_table("olist_orders_dataset"))
                products = convert_mysql_dtypes(fetch_table("olist_products_dataset"))
                sellers = convert_mysql_dtypes(fetch_table("olist_sellers_dataset"))
                category_translation = convert_mysql_dtypes(fetch_table("product_category_name_translation"))
                customers = pd.read_csv(r"data/olist_customers_dataset.csv")
                order_items = pd.read_csv(r"data/olist_order_items_dataset.csv")
                order_payments = pd.read_csv(r"data/olist_order_payments_dataset.csv")
                order_reviews = pd.read_csv(r"data/olist_order_reviews_dataset.csv")
                st.success("Data fetched and converted successfully!")
            except Exception as e:
                st.error(f"Error fetching data: {e}")
                logging.error(f"Error fetching data: {e}")
                return
            
            show_loader("Merging data...")
            merged_df = merge_ecommerce_data(orders, customers, order_payments, order_reviews, order_items, products, category_translation, sellers)
            st.success("Data merged successfully!")
            
            show_loader("Cleaning data...")
            cleaned_df = clean(merged_df)
            st.success("Data cleaned successfully!")
            
            show_loader("Converting data types...")
            st.session_state.converted_df = convert_dtypes(cleaned_df)
            st.success("Data types converted successfully!")
        
        if st.session_state.converted_df is not None:
            if st.button("Generate Star Schema"):
                show_loader("Generating star schema tables...")
                st.session_state.dim_customers, st.session_state.dim_sellers, st.session_state.dim_products, st.session_state.dim_payment_types, st.session_state.dim_reviews, st.session_state.fact_orders = generate_star_schema(st.session_state.converted_df)
                st.session_state.star_schema_generated = True
                st.sidebar.success("Star Schema Generated!")
                st.success("Star schema tables generated successfully!")
    
    elif page == "Star Schema":
        st.title("Star Schema Tables")
        if st.session_state.star_schema_generated:
            st.write("### Customers Table")
            st.write(st.session_state.dim_customers.head())
            st.write("### Sellers Table")
            st.write(st.session_state.dim_sellers.head())
            st.write("### Products Table")
            st.write(st.session_state.dim_products.head())
            st.write("### Payment Types Table")
            st.write(st.session_state.dim_payment_types.head())
            st.write("### Reviews Table")
            st.write(st.session_state.dim_reviews.head())
            st.write("### Orders Fact Table")
            st.write(st.session_state.fact_orders.head())
        else:
            st.warning("Please generate the star schema first.")
    
    elif page == "Upload to BigQuery":
        st.title("Upload to BigQuery")
        project_id = st.text_input("Enter Google Cloud Project ID:")
        dataset_id = st.text_input("Enter BigQuery Dataset ID:")
        
        if st.session_state.star_schema_generated:
            if st.button("Upload Star Schema to BigQuery"):
                if project_id and dataset_id:
                    show_loader("Uploading tables to BigQuery...")
                    upload_to_bigquery(st.session_state.fact_orders, "fact_orders", project_id, dataset_id)
                    upload_to_bigquery(st.session_state.dim_customers, "dim_customers", project_id, dataset_id)
                    upload_to_bigquery(st.session_state.dim_sellers, "dim_sellers", project_id, dataset_id)
                    upload_to_bigquery(st.session_state.dim_products, "dim_products", project_id, dataset_id)
                    upload_to_bigquery(st.session_state.dim_payment_types, "dim_payment_types", project_id, dataset_id)
                    upload_to_bigquery(st.session_state.dim_reviews, "dim_reviews", project_id, dataset_id)
                    st.success("All tables uploaded successfully!")
                else:
                    st.error("Please provide valid Project ID and Dataset ID.")
        else:
            st.warning("Please generate the star schema before uploading.")

if __name__ == "__main__":
    main()