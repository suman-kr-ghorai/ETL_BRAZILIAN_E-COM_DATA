import streamlit as st
import logging
import time
import pandas as pd
from google.cloud import bigquery
from utils.kaggle_utils import fetch_data
from utils.mysql_fetch_utils import fetch_table
from utils.convert_mysql_dtypes import convert_mysql_dtypes 
from utils.merge_df import merge_ecommerce_data
from utils.cleaner_utils import clean
from utils.convert_dtypes import convert_dtypes
from utils.bigquery_upload_utils import upload_to_bigquery
from utils.schema_utils import generate_star_schema
from utils.aggregate_utils import create_aggregation_tables, fetch_aggregation_table, get_aggregation_tables
from utils.load_datamart_utils import run_datamart_pipeline

st.set_page_config(page_title="ETL Dashboard", layout="wide")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Home", "Star Schema", "Upload to BigQuery", "Aggregated Metrics"])

def show_loader(message):
    with st.spinner(message):
        time.sleep(1)

# Initialize session state variables
if 'converted_df' not in st.session_state:
    st.session_state.converted_df = None
if 'star_schema_generated' not in st.session_state:
    st.session_state.star_schema_generated = False
if 'aggregated_metrics_generated' not in st.session_state:
    st.session_state.aggregated_metrics_generated = False
if 'datamarts_generated' not in st.session_state:
    st.session_state.datamarts_generated = False

def main():
    if page == "Home":
        st.title("ETL Pipeline Dashboard")

        if st.button("Start Extraction"):
            show_loader("Fetching data from Kaggle...")
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
                st.session_state.dim_customers, st.session_state.dim_sellers, st.session_state.dim_products, \
                st.session_state.dim_payment_types, st.session_state.dim_reviews, st.session_state.fact_orders = generate_star_schema(st.session_state.converted_df)
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
            if st.button("Upload Star Schema & Generate Data Marts"):
                if project_id and dataset_id:
                    tables_to_upload = {
                        "fact_orders": st.session_state.fact_orders,
                        "dim_customers": st.session_state.dim_customers,
                        "dim_sellers": st.session_state.dim_sellers,
                        "dim_products": st.session_state.dim_products,
                        "dim_payment_types": st.session_state.dim_payment_types,
                        "dim_reviews": st.session_state.dim_reviews,
                    }

                    for table_name, table_data in tables_to_upload.items():
                        show_loader(f"Uploading {table_name} to BigQuery...")
                        upload_to_bigquery(table_data, table_name, project_id, dataset_id)
                        st.success(f"{table_name} uploaded successfully!")

                    show_loader("Executing Aggregated Metrics...")
                    create_aggregation_tables(project_id, dataset_id)
                    st.session_state.aggregated_metrics_generated = True
                    st.sidebar.success("Aggregated Metrics Generated!")
                    st.success("Aggregated Metrics tables created successfully!")

                    show_loader("Creating Data Marts...")
                    run_datamart_pipeline(st.session_state.converted_df, project_id, dataset_id)
                    st.session_state.datamarts_generated = True
                    st.sidebar.success("Data Marts Created!")
                    st.success("Data Marts successfully created and uploaded!")

                else:
                    st.error("Please provide valid Project ID and Dataset ID.")
        else:
            st.warning("Please generate the star schema before uploading.")

    elif page == "Aggregated Metrics":
        st.title("Aggregated Metrics Tables")

        project_id = st.text_input("Enter Google Cloud Project ID:", key="project_id_input")
        dataset_id = st.text_input("Enter BigQuery Dataset ID:", key="dataset_id_input")

        if st.button("Fetch Aggregation Tables"):
            if project_id and dataset_id:
                show_loader("Fetching available aggregation tables...")
                tables = get_aggregation_tables(project_id, dataset_id)

                if tables:
                    st.session_state["aggregation_tables"] = tables
                    st.success("Aggregation tables fetched successfully!")
                else:
                    st.error("No aggregation tables found or an error occurred.")
            else:
                st.error("Please enter Project ID and Dataset ID.")

        if "aggregation_tables" in st.session_state:
            selected_table = st.selectbox("Select an Aggregation Table:", st.session_state["aggregation_tables"])

            if selected_table:
                if st.button("Fetch Table Data"):
                    show_loader(f"Fetching data from {selected_table}...")
                    df = fetch_aggregation_table(project_id, dataset_id, selected_table)

                    if df is not None and not df.empty:
                        st.write(f"### {selected_table} Data")
                        st.dataframe(df)
                    else:
                        st.error(f"Error fetching data from {selected_table} or table is empty.")
        else:
            st.warning("Click 'Fetch Aggregation Tables' to load available tables.")

if __name__ == "__main__":
    main()

