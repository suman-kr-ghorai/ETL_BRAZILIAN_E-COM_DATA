import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# Function to fetch data from BigQuery
def fetch_aggregation_table(project_id, dataset_id, table_name):
    try:
        client = bigquery.Client(project=project_id)  # Explicitly pass project_id
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error fetching {table_name}: {e}")
        return None

# Function to display insights on Agg_sales_by_category
def display_sales_by_category(df):
    st.subheader("Sales by Product Category")
    fig = px.bar(df, x="category", y="total_sales", 
                 color="category", title="Total Sales by Product Category", 
                 labels={"total_sales": "Sales Amount"})
    st.plotly_chart(fig)

# Function to display insights on Agg_sales_by_customer_state
def display_sales_by_customer_state(df):
    st.subheader("Sales by Customer State")
    fig = px.bar(df, x="customer_state", y="total_sales", 
                 color="customer_state", title="Total Sales by Customer State", 
                 labels={"total_sales": "Sales Amount"})
    st.plotly_chart(fig)

# Function to display insights on Agg_sales_by_seller
def display_sales_by_seller(df):
    st.subheader("Sales by Seller")
    fig = px.bar(df, x="seller_id", y="total_sales", 
                 color="seller_state", title="Total Sales by Seller", 
                 labels={"total_sales": "Sales Amount"})
    st.plotly_chart(fig)

# Function to display insights on Agg_monthly_sales_trend
def display_monthly_sales_trend(df):
    st.subheader("Monthly Sales Trend")
    fig = px.line(df, x="month", y="total_sales", 
                  title="Monthly Sales Trend", 
                  labels={"total_sales": "Sales Amount"})
    st.plotly_chart(fig)

# Function to display insights on Agg_avg_review_score
def display_avg_review_score(df):
    st.subheader("Average Review Score by Product")
    fig = px.bar(df, x="product_id", y="avg_review_score", 
                 color="category", title="Average Review Score by Product", 
                 labels={"avg_review_score": "Average Review Score"})
    st.plotly_chart(fig)

# Main function to create the Streamlit dashboard
def create_dashboard(project_id, dataset_id):
    st.title("Aggregation Table Insights")

    # Fetch data for each aggregation table
    tables = [
        "Agg_sales_by_category", 
        "Agg_sales_by_customer_state", 
        "Agg_sales_by_seller", 
        "Agg_monthly_sales_trend", 
        "Agg_avg_review_score"
    ]
    
    for table in tables:
        df = fetch_aggregation_table(project_id, dataset_id, table)
        if df is not None:
            if table == "Agg_sales_by_category":
                display_sales_by_category(df)
            elif table == "Agg_sales_by_customer_state":
                display_sales_by_customer_state(df)
            elif table == "Agg_sales_by_seller":
                display_sales_by_seller(df)
            elif table == "Agg_monthly_sales_trend":
                display_monthly_sales_trend(df)
            elif table == "Agg_avg_review_score":
                display_avg_review_score(df)
        else:
            st.write(f"No data available for {table}")

if __name__ == "__main__":
    # Replace with your project and dataset
    project_id = "brazilian-ecom"
    dataset_id = "t2"
    create_dashboard(project_id, dataset_id)
