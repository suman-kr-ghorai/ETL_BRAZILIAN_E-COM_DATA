import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px

# Google BigQuery Configuration
PROJECT_ID = "brazilian-ecom"
DATASET_ID = "t2"

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

# Function to get available aggregation tables
def get_aggregation_tables():
    query = f"""
        SELECT table_name
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'Agg_%'
    """
    query_job = client.query(query)
    return [row.table_name for row in query_job.result()]

# Function to fetch data from a selected table
def fetch_aggregation_table(table_name):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
    df = client.query(query).to_dataframe()
    return df

# Custom CSS Styling
st.markdown(
    """
    <style>
        body { background-color: #f4f4f4; }
        .sidebar .sidebar-content { background-color: #2e3b4e; }
        h1 { color: #ff4b4b; }
        .stDataFrame { border-radius: 10px; }
        .stPlotlyChart { border-radius: 10px; box-shadow: 0px 4px 6px rgba(0,0,0,0.1); }
    </style>
    """,
    unsafe_allow_html=True
)

# Streamlit UI
st.title("📊 BigQuery Aggregation Insights")
st.sidebar.header("🔍 Explore Data")

# Fetch available tables
tables = get_aggregation_tables()
if tables:
    selected_table = st.sidebar.selectbox("Select an Aggregation Table", tables, index=0)

    if selected_table:
        st.markdown(f"### 📂 Data from `{selected_table}`")
        
        # Fetch and display data
        df = fetch_aggregation_table(selected_table)
        with st.expander("🔍 View Raw Data"):
            st.dataframe(df, height=300, use_container_width=True)
        
        # Layout for charts
        col1, col2 = st.columns(2)
        
        # Insights based on table selection
        if selected_table == "Agg_sales_by_category":
            st.subheader("💰 Sales by Category")
            fig = px.bar(df, x="category", y="total_sales", title="Total Sales by Category", color="category")
            st.plotly_chart(fig, use_container_width=True)
            
            with col1:
                st.subheader("🚚 Freight Cost vs Sales")
                fig = px.scatter(df, x="total_sales", y="total_freight", size="total_orders", color="category",
                                 title="Freight Cost vs Total Sales")
                st.plotly_chart(fig, use_container_width=True)

        elif selected_table == "Agg_sales_by_customer_state":
            st.subheader("🗺️ Sales by Customer State")
            fig = px.bar(df, x="customer_state", y="total_sales", title="Total Sales by Customer State", color="customer_state")
            st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("👥 Unique Customers per State")
                fig = px.bar(df, x="customer_state", y="unique_customers", title="Unique Customers per State", color="customer_state")
                st.plotly_chart(fig, use_container_width=True)

        elif selected_table == "Agg_sales_by_seller":
            st.subheader("🏪 Sales by Seller")
            fig = px.bar(df, x="seller_id", y="total_sales", title="Total Sales by Seller", color="seller_state")
            st.plotly_chart(fig, use_container_width=True)
            
            with col1:
                st.subheader("🌎 Sales by Seller State")
                fig = px.pie(df, names="seller_state", values="total_sales", title="Sales Distribution by Seller State")
                st.plotly_chart(fig, use_container_width=True)

        elif selected_table == "Agg_monthly_sales_trend":
            st.subheader("📈 Monthly Sales Trend")
            fig = px.line(df, x="month", y="total_sales", title="Total Sales Trend Over Time", markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("📅 Monthly Orders Trend")
                fig = px.line(df, x="month", y="total_orders", title="Total Orders Trend Over Time", markers=True)
                st.plotly_chart(fig, use_container_width=True)

        elif selected_table == "Agg_avg_review_score":
            st.subheader("⭐ Average Review Score by Category")
            fig = px.bar(df, x="category", y="avg_review_score", title="Average Review Score by Category", color="category")
            st.plotly_chart(fig, use_container_width=True)
            
            with col1:
                st.subheader("📝 Number of Reviews by Category")
                fig = px.bar(df, x="category", y="total_reviews", title="Total Reviews by Category", color="category")
                st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.warning("No aggregation tables found.")
