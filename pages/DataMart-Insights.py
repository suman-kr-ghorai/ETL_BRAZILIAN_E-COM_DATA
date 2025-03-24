import streamlit as st
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import altair as alt
import matplotlib.pyplot as plt
import seaborn as sns
from PIL import Image
import json

# Page configuration
st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #333;
        margin-top: 1rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f5f5f5;
        border-radius: 5px;
        padding: 1rem;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Function to initialize BigQuery client
@st.cache_resource(show_spinner=False)
def get_bigquery_client():
    # Get project and dataset from environment variables
    project_id = os.environ.get("PROJECT_ID")
    
    if not project_id:
        st.error("PROJECT_ID environment variable not set")
        return None
    
    try:
        # Try using application default credentials
        client = bigquery.Client(project=project_id)
        return client
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {e}")
        return None

# Function to execute BigQuery query
@st.cache_data(ttl=3600, show_spinner=True)
def run_query(_client, query):  # Prefix client with an underscore
    if not _client:
        return None
    
    try:
        query_job = _client.query(query)
        results = query_job.result()
        return results.to_dataframe()
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        return None

# Initialize BigQuery client
client = get_bigquery_client()
project_id = os.environ.get("PROJECT_ID", "")
dataset_id = os.environ.get("DATASET_ID", "")

if not client or not project_id or not dataset_id:
    st.error("Failed to initialize. Check if PROJECT_ID and DATASET_ID environment variables are set.")
    st.stop()

# Dashboard title
st.markdown("<h1 class='main-header'>E-Commerce Analytics Dashboard</h1>", unsafe_allow_html=True)

# Sidebar 
# st.sidebar.image("https://via.placeholder.com/150x80?text=E-Commerce+Logo", width=150)
# st.sidebar.title("Navigation")

# Sidebar navigation
pages = ["Overview", "Sales Analysis", "Customer Insights", "Seller Performance", "Product Reviews"]
selected_page = st.sidebar.radio("Go to", pages)

# Display environment info
with st.sidebar.expander("Environment Info"):
    st.write(f"Project ID: {project_id}")
    st.write(f"Dataset ID: {dataset_id}")

st.sidebar.markdown("---")
st.sidebar.info("Dashboard created with Streamlit and BigQuery")

# Main content based on selected page
if selected_page == "Overview":
    st.markdown("<h2 class='section-header'>Business Overview</h2>", unsafe_allow_html=True)
    
    # Create metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    # Monthly sales trend
    monthly_sales_query = f"""
    SELECT month, total_sales, total_orders 
    FROM `{project_id}.{dataset_id}.dm_monthly_sales_trend`
    ORDER BY month
    """
    monthly_df = run_query(client, monthly_sales_query)
    
    if monthly_df is not None and not monthly_df.empty:
        # Calculate metrics
        total_sales = monthly_df['total_sales'].sum()
        total_orders = monthly_df['total_orders'].sum()
        average_order_value = total_sales / total_orders if total_orders > 0 else 0
        
        # Get category data
        category_query = f"""
        SELECT category, total_sales
        FROM `{project_id}.{dataset_id}.dm_sales_by_category`
        ORDER BY total_sales DESC
        LIMIT 1
        """
        category_df = run_query(client, category_query)
        top_category = category_df['category'].iloc[0] if not category_df.empty else "N/A"
        
        # Display metrics
        with col1:
            st.metric("Total Sales", f"${total_sales:,.2f}")
        with col2:
            st.metric("Total Orders", f"{total_orders:,}")
        with col3:
            st.metric("Avg Order Value", f"${average_order_value:.2f}")
        with col4:
            st.metric("Top Category", top_category)
        
        # Monthly trend chart
        st.markdown("<h3 class='section-header'>Sales Trend Over Time</h3>", unsafe_allow_html=True)
        
        # Convert month to date for better plotting
        monthly_df['month_date'] = pd.to_datetime(monthly_df['month'] + '-01')
        
        fig = px.line(
            monthly_df, 
            x='month_date', 
            y='total_sales',
            title='Monthly Sales Trend',
            labels={'month_date': 'Month', 'total_sales': 'Sales ($)'},
            template='plotly_white'
        )
        fig.update_layout(
            xaxis_title='Month',
            yaxis_title='Sales ($)',
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Top categories side by side with customer states
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<h3 class='section-header'>Top Product Categories</h3>", unsafe_allow_html=True)
            
            category_query = f"""
            SELECT category, total_sales
            FROM `{project_id}.{dataset_id}.dm_sales_by_category`
            ORDER BY total_sales DESC
            LIMIT 10
            """
            category_df = run_query(client, category_query)
            
            if category_df is not None and not category_df.empty:
                fig = px.bar(
                    category_df,
                    y='category',
                    x='total_sales',
                    orientation='h',
                    title='Top 10 Categories by Sales',
                    labels={'category': 'Category', 'total_sales': 'Sales ($)'},
                    template='plotly_white'
                )
                fig.update_layout(
                    yaxis={'categoryorder':'total ascending'},
                    xaxis_title='Sales ($)',
                    yaxis_title='Category',
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
        
       

elif selected_page == "Sales Analysis":
    st.markdown("<h2 class='section-header'>Sales Analysis</h2>", unsafe_allow_html=True)
    
    # Category sales analysis
    st.markdown("<h3 class='section-header'>Sales by Product Category</h3>", unsafe_allow_html=True)
    
    category_query = f"""
    SELECT category, total_sales, total_orders, total_freight
    FROM `{project_id}.{dataset_id}.dm_sales_by_category`
    ORDER BY total_sales DESC
    """
    category_df = run_query(client, category_query)
    
    if category_df is not None and not category_df.empty:
        # Calculate profit metrics (estimated)
        category_df['avg_order_value'] = category_df['total_sales'] / category_df['total_orders']
        category_df['freight_percentage'] = (category_df['total_freight'] / category_df['total_sales']) * 100
        
        # Allow user to choose metric
        metric_options = {
            'Total Sales': 'total_sales',
            'Total Orders': 'total_orders',
            'Average Order Value': 'avg_order_value',
            'Freight Cost %': 'freight_percentage'
        }
        selected_metric = st.selectbox('Select Metric:', list(metric_options.keys()))
        metric_col = metric_options[selected_metric]
        
        # Filter to top categories
        top_n = st.slider('Number of categories to display:', 5, 20, 10)
        
        # Prepare data for visualization
        plot_df = category_df.sort_values(metric_col, ascending=False).head(top_n)
        
        # Create appropriate chart based on metric
        if selected_metric == 'Freight Cost %':
            fig = px.bar(
                plot_df,
                x='category',
                y=metric_col,
                title=f'Top {top_n} Categories by {selected_metric}',
                labels={'category': 'Category', metric_col: selected_metric},
                template='plotly_white',
                color=metric_col,
                color_continuous_scale=px.colors.sequential.OrRd_r
            )
        else:
            fig = px.bar(
                plot_df,
                x='category',
                y=metric_col,
                title=f'Top {top_n} Categories by {selected_metric}',
                labels={'category': 'Category', metric_col: selected_metric},
                template='plotly_white',
                color=metric_col,
                color_continuous_scale=px.colors.sequential.Blues
            )
        
        fig.update_layout(
            xaxis={'categoryorder':'total descending', 'tickangle': 45},
            xaxis_title='Category',
            yaxis_title=selected_metric,
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Show data table with details
        with st.expander("View Detailed Data"):
            st.dataframe(
                category_df.sort_values(metric_col, ascending=False).reset_index(drop=True),
                use_container_width=True
            )
        
        # Monthly sales trend analysis
        st.markdown("<h3 class='section-header'>Monthly Sales Trends</h3>", unsafe_allow_html=True)
        
        monthly_query = f"""
        SELECT month, total_sales, total_orders
        FROM `{project_id}.{dataset_id}.dm_monthly_sales_trend`
        ORDER BY month
        """
        monthly_df = run_query(client, monthly_query)
        
        if monthly_df is not None and not monthly_df.empty:
            # Convert month to date for better plotting
            monthly_df['month_date'] = pd.to_datetime(monthly_df['month'] + '-01')
            monthly_df['avg_order_value'] = monthly_df['total_sales'] / monthly_df['total_orders']
            
            # Create tabs for different visualizations
            tab1, tab2, tab3 = st.tabs(["Sales Trend", "Order Volume", "Average Order Value"])
            
            with tab1:
                # Sales trend
                fig = px.line(
                    monthly_df, 
                    x='month_date', 
                    y='total_sales',
                    title='Monthly Sales Trend',
                    labels={'month_date': 'Month', 'total_sales': 'Sales ($)'},
                    template='plotly_white'
                )
                fig.update_layout(
                    xaxis_title='Month',
                    yaxis_title='Sales ($)',
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Add year-over-year or month-over-month growth analysis if data spans multiple years
                if len(monthly_df['month_date'].dt.year.unique()) > 1:
                    st.subheader("Year-over-Year Growth")
                    monthly_df['year'] = monthly_df['month_date'].dt.year
                    monthly_df['month_num'] = monthly_df['month_date'].dt.month
                    
                    pivot_df = monthly_df.pivot(index='month_num', columns='year', values='total_sales')
                    st.line_chart(pivot_df)
            
            with tab2:
                # Order volume
                fig = px.bar(
                    monthly_df, 
                    x='month_date', 
                    y='total_orders',
                    title='Monthly Order Volume',
                    labels={'month_date': 'Month', 'total_orders': 'Number of Orders'},
                    template='plotly_white'
                )
                fig.update_layout(
                    xaxis_title='Month',
                    yaxis_title='Number of Orders',
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with tab3:
                # Average order value
                fig = px.line(
                    monthly_df, 
                    x='month_date', 
                    y='avg_order_value',
                    title='Monthly Average Order Value',
                    labels={'month_date': 'Month', 'avg_order_value': 'Average Order Value ($)'},
                    template='plotly_white'
                )
                fig.update_layout(
                    xaxis_title='Month',
                    yaxis_title='Average Order Value ($)',
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.error("No data available. Please check if the data marts have been created.")

elif selected_page == "Customer Insights":
    st.markdown("<h2 class='section-header'>Customer Insights</h2>", unsafe_allow_html=True)
    
    # Customer state analysis
    customer_query = f"""
    SELECT customer_state, total_sales, unique_customers, total_orders
    FROM `{project_id}.{dataset_id}.dm_sales_by_customer_state`
    ORDER BY total_sales DESC
    """
    customer_df = run_query(client, customer_query)
    
    if customer_df is not None and not customer_df.empty:
        # Calculate additional metrics
        customer_df['orders_per_customer'] = customer_df['total_orders'] / customer_df['unique_customers']
        customer_df['avg_order_value'] = customer_df['total_sales'] / customer_df['total_orders']
        customer_df['revenue_per_customer'] = customer_df['total_sales'] / customer_df['unique_customers']
        
        # Create tabs for different analyses
        tab1, tab2, tab3 = st.tabs(["Geographic Distribution", "Customer Behavior", "Customer Value"])
        
        with tab1:
            st.markdown("<h3 class='section-header'>Sales by Customer Location</h3>", unsafe_allow_html=True)
            
           
            # Bar chart of top states
            top_states = customer_df.sort_values('total_sales', ascending=False).head(10)
            fig = px.bar(
                top_states,
                x='customer_state',
                y='total_sales',
                title='Top 10 States by Sales',
                labels={'customer_state': 'State', 'total_sales': 'Sales ($)'},
                template='plotly_white',
                color='total_sales',
                color_continuous_scale=px.colors.sequential.Blues
            )
            fig.update_layout(
                xaxis_title='State',
                yaxis_title='Sales ($)',
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            st.markdown("<h3 class='section-header'>Customer Behavior by State</h3>", unsafe_allow_html=True)
            
            # Metrics selection
            behavior_metric = st.selectbox(
                'Select metric:',
                ['Orders per Customer', 'Total Orders', 'Unique Customers']
            )
            
            if behavior_metric == 'Orders per Customer':
                metric_col = 'orders_per_customer'
                title = 'Average Orders per Customer by State'
                y_label = 'Orders per Customer'
            elif behavior_metric == 'Total Orders':
                metric_col = 'total_orders'
                title = 'Total Orders by State'
                y_label = 'Number of Orders'
            else:
                metric_col = 'unique_customers'
                title = 'Unique Customers by State'
                y_label = 'Number of Customers'
            
            # Sorted bar chart
            sorted_df = customer_df.sort_values(metric_col, ascending=False)
            fig = px.bar(
                sorted_df,
                x='customer_state',
                y=metric_col,
                title=title,
                labels={'customer_state': 'State', metric_col: y_label},
                template='plotly_white',
                color=metric_col,
                color_continuous_scale=px.colors.sequential.Viridis
            )
            fig.update_layout(
                xaxis_title='State',
                yaxis_title=y_label,
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Correlation analysis
            # st.subheader("Correlation Between Metrics")
            # corr_df = customer_df[['total_sales', 'unique_customers', 'total_orders', 'orders_per_customer']]
            # corr_matrix = corr_df.corr()
            
            # fig = px.imshow(
            #     corr_matrix,
            #     text_auto=True,
            #     color_continuous_scale='RdBu_r',
            #     title='Correlation Matrix of Customer Metrics'
            # )
            # st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            st.markdown("<h3 class='section-header'>Customer Value Analysis</h3>", unsafe_allow_html=True)
            
            # Customer value metric selection
            value_metric = st.selectbox(
                'Select customer value metric:',
                ['Revenue per Customer', 'Average Order Value']
            )
            
            if value_metric == 'Revenue per Customer':
                metric_col = 'revenue_per_customer'
                title = 'Average Revenue per Customer by State'
                y_label = 'Revenue per Customer ($)'
            else:
                metric_col = 'avg_order_value'
                title = 'Average Order Value by State'
                y_label = 'Average Order Value ($)'
            
            # Sorted bar chart
            sorted_df = customer_df.sort_values(metric_col, ascending=False)
            fig = px.bar(
                sorted_df,
                x='customer_state',
                y=metric_col,
                title=title,
                labels={'customer_state': 'State', metric_col: y_label},
                template='plotly_white',
                color=metric_col,
                color_continuous_scale=px.colors.sequential.Greens
            )
            fig.update_layout(
                xaxis_title='State',
                yaxis_title=y_label,
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Scatter plot comparing metrics
            # fig = px.scatter(
            #     customer_df,
            #     x='unique_customers',
            #     y='revenue_per_customer',
            #     size='total_sales',
            #     color='orders_per_customer',
            #     hover_name='customer_state',
            #     title='Customer Value vs. Customer Base Size',
            #     labels={
            #         'unique_customers': 'Number of Customers',
            #         'revenue_per_customer': 'Revenue per Customer ($)',
            #         'orders_per_customer': 'Orders per Customer'
            #     },
            #     template='plotly_white',
            #     color_continuous_scale=px.colors.sequential.Viridis
            # )
            # fig.update_layout(
            #     height=600,
            #     xaxis_title='Number of Customers',
            #     yaxis_title='Revenue per Customer ($)'
            # )
            # st.plotly_chart(fig, use_container_width=True)
    else:
        st.error("No customer data available. Please check if the data marts have been created.")

elif selected_page == "Seller Performance":
    st.markdown("<h2 class='section-header'>Seller Performance</h2>", unsafe_allow_html=True)
    
    # Seller analysis
    seller_query = f"""
    SELECT seller_id, seller_state, total_sales, total_orders
    FROM {project_id}.{dataset_id}.dm_sales_by_seller
    ORDER BY total_sales DESC
    """
    seller_df = run_query(client, seller_query)
    
    if seller_df is not None and not seller_df.empty:
        # Calculate additional metrics
        seller_df['avg_order_value'] = seller_df['total_sales'] / seller_df['total_orders']
        
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Sellers", f"{len(seller_df):,}")
        with col2:
            st.metric("Top Seller Sales", f"${seller_df['total_sales'].max():,.2f}")
        with col3:
            st.metric("Average Sales per Seller", f"${seller_df['total_sales'].mean():,.2f}")
        
        # Create tab for Top Performers only
        tab2 = st.tabs(["Top Performers"])[0]
        
        with tab2:
            st.markdown("<h3 class='section-header'>Top Performing Sellers</h3>", unsafe_allow_html=True)
            
            # Number of top sellers to show
            top_n = st.slider("Number of top sellers to display:", 5, 50, 20)
            
            # Pareto analysis
            top_sellers = seller_df.sort_values('total_sales', ascending=False).head(top_n)
            
            fig = px.bar(
                top_sellers,
                x='seller_id',
                y='total_sales',
                title=f'Top {top_n} Sellers by Sales',
                labels={'seller_id': 'Seller ID', 'total_sales': 'Sales ($)'},
                template='plotly_white',
                color='seller_state',
                hover_data=['total_orders', 'avg_order_value']
            )
            fig.update_layout(
                xaxis_title='Seller ID',
                yaxis_title='Sales ($)',
                xaxis={'tickangle': 45},
                height=600
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Sales distribution
            st.subheader("Sales Distribution Among Sellers")
            
            # Calculate percentiles
            percentiles = [0, 0.5, 0.8, 0.9, 0.95, 0.99, 1]
            percentile_values = [seller_df['total_sales'].quantile(p) for p in percentiles]
            percentile_labels = ['Min', '50th', '80th', '90th', '95th', '99th', 'Max']
            
            # Create percentile table
            percentile_df = pd.DataFrame({
                'Percentile': percentile_labels,
                'Sales Value ($)': [f"${v:,.2f}" for v in percentile_values]
            })
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.table(percentile_df)
            
            # with col2:
            #     # Histogram of seller sales
            #     fig = px.histogram(
            #         seller_df,
            #         x='total_sales',
            #         nbins=50,
            #         title='Distribution of Seller Sales',
            #         labels={'total_sales': 'Sales ($)'},
            #         template='plotly_white'
            #     )
            #     fig.update_layout(
            #         xaxis_title='Sales ($)',
            #         yaxis_title='Number of Sellers'
            #     )
            #     st.plotly_chart(fig, use_container_width=True)
            
            # Show detailed data table
            with st.expander("View Detailed Top Seller Data"):
                st.dataframe(
                    top_sellers.reset_index(drop=True),
                    use_container_width=True
                )

            st.markdown("""
                        # Sales Distribution Among Sellers
                        
                        - **Minimum Sale:** $3.50 *(the lowest recorded sale)*
                        - **50th Percentile (Median):** $849.60 *(half of the sellers make less than this, and half make more)*
                        - **80th Percentile:** $4,759.87 *(top 20% of sellers make more than this)*
                        - **90th Percentile:** $9,987.21 *(top 10% of sellers make more than this)*
                        - **95th Percentile:** $17,469.85 *(top 5% of sellers make more than this)*
                        - **99th Percentile:** $55,620.46 *(only 1% of sellers make more than this)*
                        - **Maximum Sale:** $242,591.55 *(the highest recorded sale)*
                        
                        ---
                        
                        This data suggests that the majority of sellers earn relatively modest amounts, while a small percentage of top sellers make significantly more. 
                        
                        The large jump between the **95th and 99th percentiles** (and even more so between the **99th percentile and the max**) indicates a highly skewed distribution, where a few sellers dominate the higher earnings.
                        """)
    else:
        st.error("No seller data available. Please check if the data marts have been created.")

elif selected_page == "Product Reviews":
    st.markdown("<h2 class='section-header'>Product Reviews Analysis</h2>", unsafe_allow_html=True)
    
    # Review analysis
    review_query = f"""
    SELECT category, avg_review_score, total_reviews
    FROM `{project_id}.{dataset_id}.dm_avg_review_score`
    GROUP BY category, avg_review_score, total_reviews
    """
    review_df = run_query(client, review_query)
    
    if review_df is not None and not review_df.empty:
        # Aggregate by category
        category_reviews = review_df.groupby('category').agg({
            'avg_review_score': 'mean',
            'total_reviews': 'sum'
        }).reset_index()
        
        # Overall metrics
        # Product Reviews page (continued)
    # Overall metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Average Review Score", f"{category_reviews['avg_review_score'].mean():.2f} / 5.00")
    with col2:
        st.metric("Total Reviews", f"{int(category_reviews['total_reviews'].sum()):,}")
    with col3:
        best_category = category_reviews.loc[category_reviews['avg_review_score'].idxmax()]
        st.metric("Best Rated Category", f"{best_category['category']} ({best_category['avg_review_score']:.2f})")
    
    # Create tabs for different analyses
    tab1, tab2 = st.tabs(["Review Scores by Category", "Reviews vs. Sales Analysis"])
    
    with tab1:
        st.markdown("<h3 class='section-header'>Average Review Scores by Category</h3>", unsafe_allow_html=True)
        
        # Sort categories by average review score
        sorted_reviews = category_reviews.sort_values('avg_review_score', ascending=False)
        
        # Bar chart of review scores
        fig = px.bar(
            sorted_reviews,
            x='category',
            y='avg_review_score',
            title='Categories Ranked by Average Review Score',
            labels={'category': 'Category', 'avg_review_score': 'Average Review Score'},
            template='plotly_white',
            color='avg_review_score',
            color_continuous_scale=px.colors.diverging.RdYlGn,

            range_color=[1, 5],
            hover_data=['total_reviews']
        )
        fig.update_layout(
            xaxis_title='Category',
            yaxis_title='Average Review Score (1-5)',
            xaxis={'tickangle': 45},
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Bubble chart showing review scores and volume
        fig = px.scatter(
            category_reviews,
            x='total_reviews',
            y='avg_review_score',
            size='total_reviews',
            color='avg_review_score',
            hover_name='category',
            title='Review Scores vs. Review Volume by Category',
            labels={
                'total_reviews': 'Number of Reviews',
                'avg_review_score': 'Average Review Score'
            },
            template='plotly_white',
            color_continuous_scale=px.colors.diverging.RdYlGn,
            range_color=[1, 5],
            size_max=60
        )
        fig.update_layout(
            xaxis_title='Number of Reviews',
            yaxis_title='Average Review Score (1-5)',
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.markdown("<h3 class='section-header'>Correlation Between Reviews and Sales</h3>", unsafe_allow_html=True)
        
        # Get sales data
        sales_query = f"""
        SELECT category, total_sales
        FROM `{project_id}.{dataset_id}.dm_sales_by_category`
        """
        sales_df = run_query(client, sales_query)
        
        if sales_df is not None and not sales_df.empty:
            # Merge with reviews data
            merged_df = pd.merge(category_reviews, sales_df, on='category', how='inner')
            
            # Scatter plot of reviews vs sales
            fig = px.scatter(
                merged_df,
                x='avg_review_score',
                y='total_sales',
                size='total_reviews',
                color='avg_review_score',
                hover_name='category',
                title='Review Scores vs. Sales by Category',
                labels={
                    'avg_review_score': 'Average Review Score',
                    'total_sales': 'Total Sales ($)',
                    'total_reviews': 'Number of Reviews'
                },
                template='plotly_white',
                color_continuous_scale=px.colors.diverging.RdYlGn,
                range_color=[1, 5],
                size_max=60
            )
            fig.update_layout(
                xaxis_title='Average Review Score (1-5)',
                yaxis_title='Total Sales ($)',
                height=600
            )
            # st.plotly_chart(fig, use_container_width=True)
            
            # Calculate correlation
            correlation = merged_df['avg_review_score'].corr(merged_df['total_sales'])
            st.info(f"Correlation between review scores and sales: {correlation:.2f}")
            
            # Review score distribution
            st.subheader("Sales by Review Score Range")
            
            # Create review score bins
            merged_df['score_range'] = pd.cut(
                merged_df['avg_review_score'],
                bins=[1, 2, 3, 4, 5],
                labels=['1-2', '2-3', '3-4', '4-5']
            )
            
            # Group by score range
            score_group = merged_df.groupby('score_range').agg({
                'total_sales': 'sum',
                'category': 'count',
                'total_reviews': 'sum'
            }).reset_index()
            score_group.rename(columns={'category': 'num_categories'}, inplace=True)
            
            # Create bar chart
            fig = px.bar(
                score_group,
                x='score_range',
                y='total_sales',
                title='Total Sales by Review Score Range',
                labels={
                    'score_range': 'Review Score Range',
                    'total_sales': 'Total Sales ($)'
                },
                template='plotly_white',
                color='score_range',
                color_discrete_sequence=px.colors.diverging.RdYlGn,
                text='num_categories'
            )
            fig.update_layout(
                xaxis_title='Review Score Range',
                yaxis_title='Total Sales ($)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.error("No sales data available for correlation analysis.")
        
        # Show detailed data table
        with st.expander("View Detailed Review Data"):
            st.dataframe(
                category_reviews.sort_values('avg_review_score', ascending=False).reset_index(drop=True),
                use_container_width=True
            )
else:
    st.error("No review data available. Please check if the data marts have been created.")

# Add a footer
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #666;">
        <p>E-Commerce Analytics Dashboard | Created with Streamlit & BigQuery</p>
    </div>
    """, 
    unsafe_allow_html=True
)

# Download data functionality
if "data_download" not in st.session_state:
    st.session_state.data_download = False

with st.sidebar.expander("Download Data"):
    tables = [
        "dm_sales_by_category",
        "dm_sales_by_customer_state",
        "dm_sales_by_seller",
        "dm_monthly_sales_trend",
        "dm_avg_review_score"
    ]
    
    selected_table = st.selectbox("Select data to download:", tables)
    
    if st.button("Generate CSV"):
        st.session_state.data_download = True
        st.session_state.selected_table = selected_table
        
if st.session_state.data_download:
    query = f"""
    SELECT * FROM `{project_id}.{dataset_id}.{st.session_state.selected_table}`
    """
    download_df = run_query(client, query)
    
    if download_df is not None and not download_df.empty:
        csv = download_df.to_csv(index=False)
        
        st.sidebar.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{st.session_state.selected_table}.csv",
            mime="text/csv"
        )
        
        st.sidebar.success(f"Data for {st.session_state.selected_table} is ready for download!")
    else:
        st.sidebar.error("Failed to generate download data.")