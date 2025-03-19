import streamlit as st
import pandas as pd
from google.cloud import bigquery

# Function to fetch data from BigQuery
def fetch_data_from_bigquery(project_id, dataset_id, table_name):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
    df = client.query(query).to_dataframe()
    return df

# Function to display sales performance
def display_sales_performance(df):
    st.subheader("Sales Performance")
    st.write(f"Total Revenue: {df['total_revenue'].sum():,.2f}")
    st.write(f"Average Order Value: {df['avg_order_value'].mean():,.2f}")
    st.write(f"Total Orders: {df['total_orders'].sum()}")
    st.dataframe(df)

# Function to display product category analysis
def display_product_category_analysis(df):
    st.subheader("Product Category Analysis")
    st.write(f"Total Sales Count: {df['total_sales'].sum()}")
    st.write(f"Total Revenue: {df['total_revenue'].sum():,.2f}")
    st.write(f"Average Price: {df['avg_price'].mean():,.2f}")
    st.dataframe(df)

# Function to display customer behavior
def display_customer_behavior(df):
    st.subheader("Customer Behavior")
    st.write(f"Total Orders: {df['total_orders'].sum()}")
    st.write(f"Distinct Products: {df['distinct_products'].sum()}")
    st.write(f"Total Spent: {df['total_spent'].sum():,.2f}")
    st.dataframe(df)

# Function to display order fulfillment
def display_order_fulfillment(df):
    st.subheader("Order Fulfillment")
    # st.write(f"Average Shipping Time: {df['avg_shipping_time'].mean()/3600:.2f} hours")
    st.write(f"Total Orders: {df['total_orders'].sum()}")
    st.dataframe(df)

# Function to display payment analysis
def display_payment_analysis(df):
    st.subheader("Payment Analysis")
    st.write(f"Total Revenue: {df['total_revenue'].sum():,.2f}")
    st.write(f"Total Transactions: {df['total_transactions'].sum()}")
    st.dataframe(df)

# Create dashboard function
def create_dashboard(project_id, dataset_id):
    st.title("Data Mart Insights")

    # Fetch data for each data mart
    dm_sales_performance = fetch_data_from_bigquery(project_id, dataset_id, "dm_sales_performance")
    dm_product_category_analysis = fetch_data_from_bigquery(project_id, dataset_id, "dm_product_category_analysis")
    dm_customer_behavior = fetch_data_from_bigquery(project_id, dataset_id, "dm_customer_behavior")
    dm_order_fulfillment = fetch_data_from_bigquery(project_id, dataset_id, "dm_order_fulfillment")
    dm_payment_analysis = fetch_data_from_bigquery(project_id, dataset_id, "dm_payment_analysis")

    # Display each data mart
    display_sales_performance(dm_sales_performance)
    display_product_category_analysis(dm_product_category_analysis)
    display_customer_behavior(dm_customer_behavior)
    display_order_fulfillment(dm_order_fulfillment)
    display_payment_analysis(dm_payment_analysis)

# Main function to run the dashboard
if __name__ == "__main__":
    # Set your BigQuery project and dataset IDs
    project_id = "brazilian-ecom"  # Replace with your project ID
    dataset_id = "t2"  # Replace with your dataset ID
    create_dashboard(project_id, dataset_id)
