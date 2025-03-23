import streamlit as st
import logging
import time
import pandas as pd
import numpy as np
import os
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
from utils.kpi_upload import create_and_populate_kpi_tables
from config import PROJECT_ID, DATASET_ID

# Configure page layout and styling
st.set_page_config(
    page_title="E-Commerce ETL Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS for better UI with subtle dark mode colors
st.markdown("""
<style>
    .main .block-container {padding-top: 2rem;}
    .stProgress > div > div > div {background-color: #4f6d7a;}
    .step-complete {color: #8fbc8f; font-weight: bold;}
    .step-running {color: #a9a9a9; font-weight: bold;}
    .step-waiting {color: #555555;}
    .big-font {font-size: 20px !important;}
    .status-box {padding: 10px; border-radius: 5px; margin-bottom: 10px;}
    .status-running {background-color: #2c3e50; border: 1px solid #34495e;}
    .status-complete {background-color: #1e392a; border: 1px solid #2c5b41;}
    .sidebar .sidebar-content {background-color: #1a1a1a;}
</style>
""", unsafe_allow_html=True)

# Sidebar configuration
with st.sidebar:
    # st.title("ETL Dashboard")
    st.markdown("---")
    page = st.radio("Select a pipeline mode:", [
        "ETL - Manual (Step by Step)",
        "ETL - Automated (Full Pipeline)"
    ])
    
    st.markdown("---")
    st.markdown("### Pipeline Status")
    if 'pipeline_status' not in st.session_state:
        st.session_state.pipeline_status = {
            "extraction": "waiting",
            "transformation": "waiting",
            "schema_generation": "waiting",
            "bigquery_upload": "waiting",
            "data_marts": "waiting",
            "aggregations": "waiting"
        }
    
    for step, status in st.session_state.pipeline_status.items():
        if status == "complete":
            st.markdown(f"- ✓ {step.replace('_', ' ').title()}")
        elif status == "running":
            st.markdown(f"- → {step.replace('_', ' ').title()}")
        else:
            st.markdown(f"- · {step.replace('_', ' ').title()}")

def run_step(key, func, *args, message=None, sleep_time=None):
    """Run a step of the ETL pipeline with progress tracking"""
    if message is None:
        message = f"Running {key.replace('_', ' ').title()}"
    
    st.session_state.pipeline_status[key] = "running"
    
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    
    with status_placeholder.container():
        st.markdown(f"""
        <div class="status-box status-running">
            <h3>→ {message}...</h3>
            <p>Please wait while this operation completes.</p>
        </div>
        """, unsafe_allow_html=True)
    
    progress_bar = progress_placeholder.progress(0)
    
    # Simulate or track real progress
    if sleep_time:
        for i in range(100):
            time.sleep(sleep_time/100)
            progress_bar.progress(i + 1)
    
    result = func(*args) if args else func()
    
    st.session_state.pipeline_status[key] = "complete"
    
    with status_placeholder.container():
        st.markdown(f"""
        <div class="status-box status-complete">
            <h3>✓ {message} Completed</h3>
        </div>
        """, unsafe_allow_html=True)
    
    return result

def display_data_insights(df, title="Data Insights"):
    """Displays shape, total records, and total null values per column in a DataFrame."""
    st.subheader(title)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Records", f"{len(df):,}")
    with col2:
        st.metric("Columns", f"{df.shape[1]}")
    with col3:
        null_counts = df.isna().sum().sum()
        st.metric("Total Null Values", f"{null_counts:,}")
    
    # Display sample data
    with st.expander("View Sample Data", expanded=False):
        st.dataframe(df.head(10))
    
    # Display null values by column
    with st.expander("View Null Values by Column", expanded=False):
        null_by_col = df.isna().sum().reset_index()
        null_by_col.columns = ['Column', 'Null Count']
        null_by_col = null_by_col[null_by_col['Null Count'] > 0].sort_values('Null Count', ascending=False)
        if len(null_by_col) > 0:
            st.dataframe(null_by_col)
        else:
            st.info("No null values found in any column.")

def display_schema_image():
    """Display the schema image if it exists"""
    schema_path = "Schema_.png"
    if os.path.exists(schema_path):
        st.subheader("Star Schema Visualization")
        st.image(schema_path, use_column_width=True, caption="Star Schema Structure")
    else:
        st.warning("Schema visualization image not found. Expected at: " + schema_path)

def main():
    if page == "ETL - Manual (Step by Step)":
        st.title("ETL Pipeline - Manual Mode")
        st.markdown("""
        In this mode, you can execute each step of the ETL pipeline individually. 
        Follow the steps below in sequence for a complete pipeline run.
        """)
        
        # Step 1: Extraction
        st.markdown("## Step 1: Data Extraction")
        st.markdown("Fetch data from Kaggle and MySQL databases")
        
        if st.button("Start Extraction", key="extract_btn"):
            # Kaggle data
            run_step("extraction", fetch_data, message="Fetching data from Kaggle", sleep_time=2)
            
            # MySQL tables
            st.session_state.orders = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("olist_orders_dataset")),
                message="Fetching Orders table", 
                sleep_time=0.5
            )
            st.session_state.products = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("olist_products_dataset")),
                message="Fetching Products table", 
                sleep_time=0.5
            )
            st.session_state.sellers = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("olist_sellers_dataset")),
                message="Fetching Sellers table", 
                sleep_time=0.5
            )
            st.session_state.category_translation = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("product_category_name_translation")),
                message="Fetching Category translations", 
                sleep_time=0.5
            )
            
            # CSV files
            st.session_state.customers = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_customers_dataset.csv"),
                message="Loading Customers data", 
                sleep_time=0.5
            )
            st.session_state.order_items = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_order_items_dataset.csv"),
                message="Loading Order Items data", 
                sleep_time=0.5
            )
            st.session_state.order_payments = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_order_payments_dataset.csv"),
                message="Loading Order Payments data", 
                sleep_time=0.5
            )
            st.session_state.order_reviews = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_order_reviews_dataset.csv"),
                message="Loading Order Reviews data", 
                sleep_time=0.5
            )
            
            st.success("All data sources extracted successfully!")
        
        # Only show next steps if extraction is complete
        if "orders" in st.session_state:
            st.markdown("---")
            st.markdown("## Step 2: Data Preview and Analysis")
            
            if st.button("View Raw Data Insights", key="view_raw_btn"):
                with st.spinner("Merging datasets for analysis..."):
                    raw_data = merge_ecommerce_data(
                        st.session_state.orders, st.session_state.customers, 
                        st.session_state.order_payments, st.session_state.order_reviews, 
                        st.session_state.order_items, st.session_state.products, 
                        st.session_state.category_translation, st.session_state.sellers
                    )
                    display_data_insights(raw_data, "Raw Data Analysis")
            
            st.markdown("---")
            st.markdown("## Step 3: Data Transformation & Cleaning")
            
            if st.button("Transform & Clean Data", key="transform_btn"):
                merged_df = run_step(
                    "transformation",
                    lambda: merge_ecommerce_data(
                        st.session_state.orders, st.session_state.customers, 
                        st.session_state.order_payments, st.session_state.order_reviews, 
                        st.session_state.order_items, st.session_state.products, 
                        st.session_state.category_translation, st.session_state.sellers
                    ),
                    message="Merging datasets",
                    sleep_time=2
                )
                
                cleaned_df = run_step(
                    "transformation",
                    lambda: clean(merged_df),
                    message="Cleaning data",
                    sleep_time=1.5
                )
                
                st.session_state.converted_df = run_step(
                    "transformation",
                    lambda: convert_dtypes(cleaned_df),
                    message="Converting data types",
                    sleep_time=1
                )
                
                display_data_insights(st.session_state.converted_df, "Transformed Data Analysis")
        
        # Only show next steps if transformation is complete
        if "converted_df" in st.session_state:
            st.markdown("---")
            st.markdown("## Step 4: Star Schema Generation")
            
            if st.button("Generate Star Schema", key="schema_btn"):
                result = run_step(
                    "schema_generation",
                    lambda: generate_star_schema(st.session_state.converted_df),
                    message="Generating star schema",
                    sleep_time=3
                )
                
                st.session_state.dim_customers, st.session_state.dim_sellers, st.session_state.dim_products, \
                st.session_state.dim_payment_types, st.session_state.dim_reviews, st.session_state.fact_orders = result
                
                # Display schema table counts
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Dimensions", "5")
                    st.metric("Fact Tables", "1")
                with col2:
                    st.metric("Customer Dimension", f"{len(st.session_state.dim_customers):,} rows")
                    st.metric("Product Dimension", f"{len(st.session_state.dim_products):,} rows")
                with col3:
                    st.metric("Fact Orders", f"{len(st.session_state.fact_orders):,} rows")
                    st.metric("Review Dimension", f"{len(st.session_state.dim_reviews):,} rows")
                
                # Display the schema image
                display_schema_image()
        
        # Only show next steps if star schema is generated
        if "fact_orders" in st.session_state:
            st.markdown("---")
            st.markdown("## Step 5: BigQuery Upload & Data Marts")
            
            if st.button("Upload to BigQuery & Generate Data Marts", key="bq_btn"):
                # Upload dimension tables
                dimension_tables = {
                    "dim_customers": st.session_state.dim_customers,
                    "dim_sellers": st.session_state.dim_sellers,
                    "dim_products": st.session_state.dim_products,
                    "dim_payment_types": st.session_state.dim_payment_types,
                    "dim_reviews": st.session_state.dim_reviews
                }
                
                for table_name, table_data in dimension_tables.items():
                    run_step(
                        "bigquery_upload",
                        lambda tn=table_name, td=table_data: upload_dimension_table(td, tn, PROJECT_ID, DATASET_ID),
                        message=f"Uploading {table_name} to BigQuery",
                        sleep_time=1
                    )
                
                # Upload fact table
                run_step(
                    "bigquery_upload",
                    lambda: upload_fact_table(st.session_state.fact_orders, PROJECT_ID, DATASET_ID),
                    message="Uploading fact_orders to BigQuery",
                    sleep_time=2
                )
                
                # Create data marts
                run_step(
                    "data_marts",
                    lambda: create_datamart_tables(PROJECT_ID, DATASET_ID),
                    message="Creating data marts",
                    sleep_time=3
                )
            
            st.markdown("---")
            st.markdown("## Step 6: Aggregations & KPIs")
            
            if st.button("Create Aggregation Tables & KPIs", key="agg_btn"):
                run_step(
                    "aggregations",
                    lambda: create_aggregation_tables(PROJECT_ID, DATASET_ID),
                    message="Creating aggregation tables",
                    sleep_time=2
                )
                
                run_step(
                    "aggregations",
                    lambda: create_and_populate_kpi_tables(PROJECT_ID, DATASET_ID),
                    message="Generating KPI tables",
                    sleep_time=2
                )
                
                # Show completion message without balloons
                st.success("ETL Pipeline completed successfully")
                st.info("All data has been processed, transformed, and loaded into BigQuery. Data marts, aggregation tables, and KPIs are ready for analysis.")
    
    elif page == "ETL - Automated (Full Pipeline)":
        st.title("ETL Pipeline - Automated Mode")
        st.markdown("""
        This mode runs the entire ETL pipeline automatically from start to finish.
        Press the button below to begin the process.
        """)
        
        if st.button("Run Full ETL Pipeline", key="auto_pipeline"):
            # Reset pipeline status
            for key in st.session_state.pipeline_status:
                st.session_state.pipeline_status[key] = "waiting"
            
            # Extraction phase
            run_step(
                "extraction",
                fetch_data,
                message="Fetching external data sources",
                sleep_time=2
            )
            
            orders = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("olist_orders_dataset")),
                message="Extracting orders data", 
                sleep_time=0.5
            )
            
            products = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("olist_products_dataset")),
                message="Extracting products data", 
                sleep_time=0.5
            )
            
            sellers = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("olist_sellers_dataset")),
                message="Extracting sellers data", 
                sleep_time=0.5
            )
            
            category_translation = run_step(
                "extraction", 
                lambda: convert_mysql_dtypes(fetch_table("product_category_name_translation")),
                message="Extracting category translations", 
                sleep_time=0.5
            )
            
            customers = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_customers_dataset.csv"),
                message="Loading customers data", 
                sleep_time=0.5
            )
            
            order_items = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_order_items_dataset.csv"),
                message="Loading order items data", 
                sleep_time=0.5
            )
            
            order_payments = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_order_payments_dataset.csv"),
                message="Loading payment data", 
                sleep_time=0.5
            )
            
            order_reviews = run_step(
                "extraction", 
                lambda: pd.read_csv(r"data/olist_order_reviews_dataset.csv"),
                message="Loading reviews data", 
                sleep_time=0.5
            )
            
            # Transformation phase
            merged_df = run_step(
                "transformation",
                lambda: merge_ecommerce_data(
                    orders, customers, order_payments, order_reviews, 
                    order_items, products, category_translation, sellers
                ),
                message="Merging all datasets",
                sleep_time=2
            )
            
            cleaned_df = run_step(
                "transformation",
                lambda: clean(merged_df),
                message="Cleaning and processing data",
                sleep_time=1.5
            )
            
            converted_df = run_step(
                "transformation",
                lambda: convert_dtypes(cleaned_df),
                message="Converting data types",
                sleep_time=1
            )
            
            # Display insights after transformation
            display_data_insights(converted_df, "Data After Transformation")
            
            # Schema generation phase
            schema_result = run_step(
                "schema_generation",
                lambda: generate_star_schema(converted_df),
                message="Generating star schema",
                sleep_time=3
            )
            
            dim_customers, dim_sellers, dim_products, dim_payment_types, dim_reviews, fact_orders = schema_result
            
            # Display the schema image
            display_schema_image()
            
            # BigQuery upload phase
            dimension_tables = {
                "dim_customers": dim_customers,
                "dim_sellers": dim_sellers,
                "dim_products": dim_products,
                "dim_payment_types": dim_payment_types,
                "dim_reviews": dim_reviews
            }
            
            for table_name, table_data in dimension_tables.items():
                run_step(
                    "bigquery_upload",
                    lambda tn=table_name, td=table_data: upload_dimension_table(td, tn, PROJECT_ID, DATASET_ID),
                    message=f"Uploading {table_name} to BigQuery",
                    sleep_time=0.8
                )
            
            run_step(
                "bigquery_upload",
                lambda: upload_fact_table(fact_orders, PROJECT_ID, DATASET_ID),
                message="Uploading fact_orders to BigQuery",
                sleep_time=1.5
            )
            
            # Data marts phase
            run_step(
                "data_marts",
                lambda: create_datamart_tables(PROJECT_ID, DATASET_ID),
                message="Creating data marts",
                sleep_time=2.5
            )
            
            # Aggregations and KPIs phase
            run_step(
                "aggregations",
                lambda: create_aggregation_tables(PROJECT_ID, DATASET_ID),
                message="Creating aggregation tables",
                sleep_time=1.5
            )
            
            run_step(
                "aggregations",
                lambda: create_and_populate_kpi_tables(PROJECT_ID, DATASET_ID),
                message="Generating KPI tables",
                sleep_time=1.5
            )
            
            # Show completion message without balloons
            st.success("ETL Pipeline completed successfully")
            
            # Display summary metrics
            st.subheader("Pipeline Summary")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Tables Created", "11+")
                st.metric("Dimension Tables", "5")
            with col2:
                st.metric("Fact Tables", "1")
                st.metric("Data Marts", "3+")
            with col3:
                st.metric("Aggregation Tables", "2+")
                st.metric("KPI Tables", "1+")
            
            st.info("All data has been processed, transformed, and loaded into BigQuery. Data marts, aggregation tables, and KPIs are ready for analysis.")

if __name__ == "__main__":
    main()