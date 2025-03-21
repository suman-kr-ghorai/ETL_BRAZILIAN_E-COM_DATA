import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px

# Google BigQuery Configuration
PROJECT_ID = "brazilian-ecom"
DATASET_ID = "t6"

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

# List of available Data Marts
DATA_MARTS = {
    "Sales Performance": "dm_sales_performance",
    "Product Category Analysis": "dm_product_category_analysis",
    "Customer Behavior": "dm_customer_behavior",
    "Order Fulfillment": "dm_order_fulfillment",
    "Payment Analysis": "dm_payment_analysis",
}

# Function to fetch data from BigQuery
@st.cache_data(ttl=600)
def fetch_datamart(table_name):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
    df = client.query(query).to_dataframe()
    return df

# Streamlit UI
st.title("📊 E-Commerce Data Insights Dashboard")
st.sidebar.header("Select a Data Mart")

# Select Data Mart
selected_mart = st.sidebar.selectbox("Choose a Data Mart", list(DATA_MARTS.keys()))

if selected_mart:
    table_name = DATA_MARTS[selected_mart]
    st.subheader(f"📂 Data: `{table_name}`")

    # Fetch Data
    df = fetch_datamart(table_name)
    st.dataframe(df)

    # **📈 Insights & Visualizations**
    if table_name == "dm_sales_performance":
        st.subheader("💰 Sales Performance Insights")
        # fig = px.histogram(df, x="total_revenue", title="Total Revenue Distribution")
        # st.plotly_chart(fig)
        top_customers = df.nlargest(10, "total_revenue")
        fig = px.bar(top_customers, x="customer_id", y="total_revenue", title="Top 10 Customers by Revenue")
        st.plotly_chart(fig)

    elif table_name == "dm_product_category_analysis":
        st.subheader("📦 Product Category Analysis")
        fig = px.bar(df, x="product_category_name", y="total_sales", title="Total Sales by Product Category")
        st.plotly_chart(fig)
        fig = px.bar(df, x="product_category_name", y="avg_price", title="Average Price by Product Category")
        st.plotly_chart(fig)

    elif table_name == "dm_customer_behavior":
        st.subheader("👥 Customer Behavior Analysis")
        # fig = px.bar(df, x="customer_id", y="total_orders", title="Total Orders per Customer")
        # st.plotly_chart(fig)
        fig = px.scatter(df, x="total_orders", y="total_spent", title="Customer Spending Behavior", size="total_spent")
        st.plotly_chart(fig)

    elif table_name == "dm_order_fulfillment":
        st.subheader("🚚 Order Fulfillment Analysis")
        fig = px.pie(df, names="order_status", values="total_orders", title="Order Status Distribution")
        st.plotly_chart(fig)
        fig = px.bar(df, x="order_status", y="avg_shipping_time", title="Average Shipping Time per Order Status")
        st.plotly_chart(fig)

    elif table_name == "dm_payment_analysis":
        st.subheader("💳 Payment Analysis")
        fig = px.bar(df, x="payment_type", y="total_revenue", title="Total Revenue by Payment Type")
        st.plotly_chart(fig)
        fig = px.pie(df, names="payment_type", values="total_transactions", title="Total Transactions by Payment Type")
        st.plotly_chart(fig)
