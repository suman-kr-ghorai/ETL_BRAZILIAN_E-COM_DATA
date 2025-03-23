import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import os
from datetime import datetime
import altair as alt

# Page configuration
st.set_page_config(
    page_title="Brazilian E-commerce Analytics Dashboard",
    page_icon="🛍️",
    layout="wide"
)

# Initialize BigQuery client using environment variables
@st.cache_resource
def get_client():
    project_id = os.getenv("PROJECT_ID", "brazilian-ecom")
    return bigquery.Client(project=project_id)

client = get_client()

# Get dataset ID from environment variable
dataset_id = os.getenv("DATASET_ID", "t6")  # Default to "t6" if not set

# Load data functions
@st.cache_data(ttl=3600)
def load_orders_summary():
    query = f"""
    SELECT * FROM `{client.project}.{dataset_id}.kpi_orders_summary`
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_customer_ltv():
    query = f"""
    SELECT * FROM `{client.project}.{dataset_id}.kpi_customer_lifetime_value`
    ORDER BY total_spent DESC
    LIMIT 1000
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_seller_performance():
    query = f"""
    SELECT * FROM `{client.project}.{dataset_id}.kpi_seller_performance`
    ORDER BY total_revenue DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_yoy_revenue():
    query = f"""
    SELECT * FROM `{client.project}.{dataset_id}.kpi_yoy_revenue`
    ORDER BY year
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_yoy_seller_city():
    query = f"""
    SELECT * FROM `{client.project}.{dataset_id}.kpi_yoy_seller_city`
    ORDER BY year, total_revenue DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_yoy_product_category():
    query = f"""
    SELECT * FROM `{client.project}.{dataset_id}.kpi_yoy_product_category`
    ORDER BY year, total_revenue DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_top_cities():
    query = f"""
    SELECT seller_city, SUM(total_revenue) as total_revenue, COUNT(DISTINCT seller_key) as seller_count
    FROM `{client.project}.{dataset_id}.kpi_seller_performance`
    GROUP BY seller_city
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_top_categories():
    query = f"""
    SELECT product_category, SUM(total_revenue) as total_revenue
    FROM `{client.project}.{dataset_id}.kpi_product_performance`
    GROUP BY product_category
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    return client.query(query).to_dataframe()

# Dashboard title
st.title("🛍️ Brazilian E-commerce Analytics Dashboard")
st.markdown("This dashboard presents key performance indicators from the Brazilian e-commerce dataset.")

# Create tabs for different analysis sections
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Orders & Revenue", 
    "Customer Analysis", 
    "Seller Analysis",
    "Year-over-Year Trends",
    "Category Analysis"
])

# Tab 1: Orders & Revenue
with tab1:
    st.header("Orders and Revenue Analysis")
    
    try:
        # Load data
        orders_df = load_orders_summary()
        
        # Create metrics row
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_orders = int(orders_df['total_orders'].sum())
            st.metric("Total Orders", f"{total_orders:,}")
            
        with col2:
            total_revenue = orders_df['total_revenue'].sum()
            st.metric("Total Revenue", f"R$ {total_revenue:,.2f}")
            
        with col3:
            avg_order = orders_df['total_revenue'].sum() / orders_df['total_orders'].sum()
            st.metric("Average Order Value", f"R$ {avg_order:,.2f}")
        
        # Order Status Breakdown
        st.subheader("Order Status Breakdown")
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                orders_df, 
                values='total_orders', 
                names='order_status',
                title='Orders by Status',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            fig = px.bar(
                orders_df, 
                x='order_status', 
                y='total_revenue',
                title='Revenue by Order Status',
                color='order_status',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig.update_layout(xaxis_title="Order Status", yaxis_title="Revenue (R$)")
            st.plotly_chart(fig, use_container_width=True)
        
        # Insights
        st.subheader("Insights")
        st.markdown("""
        **Order Status Analysis:**
        - The majority of orders are in 'delivered' status, indicating a healthy fulfillment rate.
        - Cancelled orders represent a small percentage of total orders, suggesting good order qualification.
        - The average order value varies by status, with 'processing' orders showing higher values, possibly indicating larger purchases take longer to process.
        
        **Revenue Impact:**
        - Delivered orders generate the bulk of revenue, as expected.
        - The revenue lost from cancelled orders represents an opportunity for recovery strategies.
        """)
        
    except Exception as e:
        st.error(f"Error loading order summary data: {e}")

# Tab 2: Customer Analysis
with tab2:
    st.header("Customer Lifetime Value Analysis")
    
    try:
        # Load data
        customer_df = load_customer_ltv()
        
        # Customer metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_customers = len(customer_df)
            st.metric("Total Customers", f"{total_customers:,}")
            
        with col2:
            avg_ltv = customer_df['total_spent'].mean()
            st.metric("Average Customer Lifetime Value", f"R$ {avg_ltv:,.2f}")
            
        with col3:
            avg_orders_per_customer = customer_df['total_orders'].mean()
            st.metric("Avg Orders per Customer", f"{avg_orders_per_customer:.2f}")
        
        # Customer segmentation
        st.subheader("Customer Segmentation")
        
        # Create LTV segments
        customer_df['ltv_segment'] = pd.qcut(
            customer_df['total_spent'], 
            q=4, 
            labels=['Low Value', 'Medium Value', 'High Value', 'Premium']
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            segment_counts = customer_df['ltv_segment'].value_counts().reset_index()
            segment_counts.columns = ['Segment', 'Count']
            
            fig = px.pie(
                segment_counts, 
                values='Count', 
                names='Segment',
                title='Customer Segmentation by Lifetime Value',
                color_discrete_sequence=px.colors.sequential.Viridis
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            segment_revenue = customer_df.groupby('ltv_segment')['total_spent'].sum().reset_index()
            
            fig = px.bar(
                segment_revenue, 
                x='ltv_segment', 
                y='total_spent',
                title='Revenue Contribution by Customer Segment',
                color='ltv_segment',
                color_discrete_sequence=px.colors.sequential.Viridis
            )
            fig.update_layout(xaxis_title="Customer Segment", yaxis_title="Total Revenue (R$)")
            st.plotly_chart(fig, use_container_width=True)
        
        # Order frequency
        st.subheader("Order Frequency Analysis")
        
        order_freq = customer_df['total_orders'].value_counts().reset_index()
        order_freq.columns = ['Order Count', 'Customer Count']
        order_freq = order_freq.sort_values('Order Count')
        
        fig = px.bar(
            order_freq.head(10), 
            x='Order Count', 
            y='Customer Count',
            title='Customers by Order Frequency',
            color='Customer Count',
            color_continuous_scale=px.colors.sequential.Viridis
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Insights
        st.subheader("Insights")
        st.markdown("""
        **Customer Lifetime Value Analysis:**
        - The majority of customers make only 1-2 purchases, indicating an opportunity for retention strategies.
        - Premium customers (top 25% by spending) contribute disproportionately to total revenue.
        - There's significant potential to move customers from Medium to High Value segments with targeted marketing.
        
        **Recommendations:**
        - Implement a loyalty program to increase repeat purchases.
        - Create targeted offers for Medium Value customers to increase their average order value.
        - Develop specialized retention campaigns for High Value and Premium customers.
        """)
        
    except Exception as e:
        st.error(f"Error loading customer data: {e}")

# Tab 3: Seller Analysis
with tab3:
    st.header("Seller Performance Analysis")
    
    try:
        # Load data
        seller_df = load_seller_performance()
        top_cities_df = load_top_cities()
        
        # Seller metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_sellers = len(seller_df)
            st.metric("Total Sellers", f"{total_sellers:,}")
            
        with col2:
            avg_revenue_per_seller = seller_df['total_revenue'].mean()
            st.metric("Avg Revenue per Seller", f"R$ {avg_revenue_per_seller:,.2f}")
            
        with col3:
            total_cities = seller_df['seller_city'].nunique()
            st.metric("Cities with Sellers", f"{total_cities:,}")
        
        # Top cities
        st.subheader("Top Seller Cities by Revenue")
        
        fig = px.bar(
            top_cities_df,
            x='seller_city',
            y='total_revenue',
            title='Top 10 Cities by Total Revenue',
            color='total_revenue',
            color_continuous_scale=px.colors.sequential.Plasma,
            text='seller_count'
        )
        fig.update_layout(
            xaxis_title="City",
            yaxis_title="Total Revenue (R$)",
            xaxis={'categoryorder':'total descending'}
        )
        fig.update_traces(texttemplate='%{text} sellers', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
        
        # Revenue distribution
        st.subheader("Seller Revenue Distribution")
        
        # Create seller segments
        seller_df['revenue_segment'] = pd.qcut(
            seller_df['total_revenue'], 
            q=[0, 0.5, 0.8, 0.95, 1.0], 
            labels=['Small', 'Medium', 'Large', 'Enterprise']
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            segment_counts = seller_df['revenue_segment'].value_counts().reset_index()
            segment_counts.columns = ['Segment', 'Count']
            
            fig = px.pie(
                segment_counts, 
                values='Count', 
                names='Segment',
                title='Seller Segmentation by Revenue',
                color_discrete_sequence=px.colors.sequential.Plasma
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            segment_revenue = seller_df.groupby('revenue_segment')['total_revenue'].sum().reset_index()
            
            fig = px.bar(
                segment_revenue, 
                x='revenue_segment', 
                y='total_revenue',
                title='Revenue Contribution by Seller Segment',
                color='revenue_segment',
                color_discrete_sequence=px.colors.sequential.Plasma
            )
            fig.update_layout(xaxis_title="Seller Segment", yaxis_title="Total Revenue (R$)")
            st.plotly_chart(fig, use_container_width=True)
        
        # Insights
        st.subheader("Insights")
        st.markdown("""
        **Seller Distribution Analysis:**
        - A small number of Enterprise sellers (top 5%) generate a substantial portion of total revenue.
        - Sellers are concentrated in major urban centers, with São Paulo being the dominant market.
        - The long tail of Small sellers represents an opportunity for targeted growth programs.
        
        **Geographic Insights:**
        - Major cities have a higher seller-to-revenue ratio, indicating stronger market efficiency.
        - Smaller cities show potential for expansion with the right support and incentives.
        - Regional differences in seller performance suggest the need for localized strategies.
        """)
        
    except Exception as e:
        st.error(f"Error loading seller data: {e}")

# Tab 4: Year-over-Year Trends
with tab4:
    st.header("Year-over-Year Performance Analysis")
    
    try:
        # Load data
        yoy_revenue_df = load_yoy_revenue()
        yoy_city_df = load_yoy_seller_city()
        yoy_category_df = load_yoy_product_category()
        
        # YoY Revenue Trend
        st.subheader("Year-over-Year Revenue Trend")
        
        fig = px.line(
            yoy_revenue_df,
            x='year',
            y='total_revenue',
            title='Annual Revenue Trend',
            markers=True,
            line_shape='linear'
        )
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Total Revenue (R$)",
            xaxis=dict(tickmode='linear')
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate YoY growth
        yoy_revenue_df['growth'] = yoy_revenue_df['total_revenue'].pct_change() * 100
        
        # Display growth metrics
        col1, col2 = st.columns(2)
        
        with col1:
            last_year = yoy_revenue_df['year'].max()
            last_year_growth = yoy_revenue_df[yoy_revenue_df['year'] == last_year]['growth'].values[0]
            
            if pd.notnull(last_year_growth):
                st.metric(
                    f"Growth in {last_year}", 
                    f"{last_year_growth:.2f}%",
                    delta=f"{last_year_growth:.2f}%"
                )
            else:
                st.metric(f"Revenue in {last_year}", f"R$ {yoy_revenue_df[yoy_revenue_df['year'] == last_year]['total_revenue'].values[0]:,.2f}")
        
        with col2:
            avg_growth = yoy_revenue_df['growth'].mean()
            if pd.notnull(avg_growth):
                st.metric("Average Annual Growth", f"{avg_growth:.2f}%")
        
        # Top cities by year
        st.subheader("City Performance by Year")
        
        # Get top 5 cities overall
        top_cities = yoy_city_df.groupby('seller_city')['total_revenue'].sum().nlargest(5).index.tolist()
        
        # Filter for top cities
        top_cities_data = yoy_city_df[yoy_city_df['seller_city'].isin(top_cities)]
        
        fig = px.line(
            top_cities_data,
            x='year',
            y='total_revenue',
            color='seller_city',
            title='Top 5 Cities - Revenue Trend',
            markers=True,
            line_shape='linear'
        )
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Total Revenue (R$)",
            xaxis=dict(tickmode='linear')
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Insights
        st.subheader("Insights")
        st.markdown("""
        **Year-over-Year Trends:**
        - The platform shows consistent revenue growth year over year, indicating market expansion.
        - Growth rates vary by year, with more recent periods showing accelerated growth.
        - Major cities maintain their dominant position, but some secondary markets are growing faster.
        
        **Strategic Implications:**
        - The growth trajectory suggests a maturing market with increasing e-commerce adoption.
        - Year-over-year variations highlight the impact of economic factors and platform developments.
        - City-specific trends reveal opportunities for targeted regional expansion strategies.
        """)
        
    except Exception as e:
        st.error(f"Error loading YoY data: {e}")

# Tab 5: Category Analysis
with tab5:
    st.header("Product Category Analysis")
    
    try:
        # Try to load category data
        try:
            top_categories_df = load_top_categories()
        except:
            # If kpi_product_performance table doesn't exist or has errors,
            # use the YoY category data as fallback
            yoy_category_df = load_yoy_product_category()
            top_categories_df = yoy_category_df.groupby('product_category')['total_revenue'].sum().reset_index()
            top_categories_df = top_categories_df.sort_values('total_revenue', ascending=False).head(10)
        
        # Top Categories
        st.subheader("Top Product Categories by Revenue")
        
        fig = px.bar(
            top_categories_df.head(10),
            x='product_category',
            y='total_revenue',
            title='Top 10 Categories by Total Revenue',
            color='total_revenue',
            color_continuous_scale=px.colors.sequential.Viridis
        )
        fig.update_layout(
            xaxis_title="Product Category",
            yaxis_title="Total Revenue (R$)",
            xaxis={'categoryorder':'total descending'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # YoY Category Trends
        st.subheader("Category Trends Over Time")
        
        try:
            # Try to get YoY category data
            yoy_category_df = load_yoy_product_category()
            
            # Get top 5 categories
            top_cats = yoy_category_df.groupby('product_category')['total_revenue'].sum().nlargest(5).index.tolist()
            
            # Filter for top categories
            top_cats_data = yoy_category_df[yoy_category_df['product_category'].isin(top_cats)]
            
            fig = px.line(
                top_cats_data,
                x='year',
                y='total_revenue',
                color='product_category',
                title='Top 5 Categories - Revenue Trend',
                markers=True,
                line_shape='linear'
            )
            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Total Revenue (R$)",
                xaxis=dict(tickmode='linear')
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f"Unable to load category trend data: {e}")
        
        # Insights
        st.subheader("Insights")
        st.markdown("""
        **Category Performance Analysis:**
        - The top categories represent a significant portion of overall revenue, indicating category concentration.
        - Categories show different growth patterns, with some emerging categories showing rapid expansion.
        - Traditional categories maintain stable revenue streams, while trending categories show more volatility.
        
        **Category Strategy Recommendations:**
        - Invest in expanding inventory and seller recruitment in high-growth categories.
        - Consider specialized marketing campaigns for seasonal category performance.
        - Monitor emerging categories for early identification of consumer trends.
        """)
        
    except Exception as e:
        st.error(f"Error loading category data: {e}")

# Executive Summary
st.header("Executive Summary")
st.markdown("""
### Key Business Insights

1. **Overall Performance**
   - The Brazilian e-commerce platform demonstrates strong revenue growth and high order fulfillment rates.
   - Customer acquisition appears effective, but retention metrics suggest opportunities for improvement.

2. **Customer Dynamics**
   - Customer segmentation reveals that a small percentage of high-value customers drive a disproportionate share of revenue.
   - Most customers are one-time purchasers, indicating significant potential for retention strategies.

3. **Seller Ecosystem**
   - Seller distribution shows concentration in major urban centers, with São Paulo dominating the market.
   - Enterprise sellers (top 5%) generate the majority of platform revenue, while the long tail of small sellers represents growth potential.

4. **Product Categories**
   - Certain product categories consistently outperform others, suggesting potential for category-specific strategies.
   - Year-over-year category trends show shifting consumer preferences and opportunities for inventory optimization.

5. **Regional Analysis**
   - Geographic performance varies significantly, with major cities showing stronger seller performance metrics.
   - Regional growth rates differ, indicating opportunities for targeted expansion strategies.

### Strategic Recommendations

1. **Customer Retention Focus**
   - Implement a loyalty program to convert one-time buyers into repeat customers.
   - Develop personalized marketing campaigns for medium-value customers to increase their lifetime value.

2. **Seller Development**
   - Create specialized support programs for small and medium sellers to improve their performance.
   - Expand seller recruitment in high-potential secondary markets.

3. **Category Optimization**
   - Increase inventory and marketing for high-growth categories.
   - Develop category-specific promotional strategies based on seasonal performance data.

4. **Regional Expansion**
   - Target underserved regions with focused marketing and seller incentives.
   - Develop localized strategies for different regions based on performance data.
""")

# Footer
st.markdown("---")
st.markdown(f"Brazilian E-commerce Analytics Dashboard | Project: {client.project} | Dataset: {dataset_id}")