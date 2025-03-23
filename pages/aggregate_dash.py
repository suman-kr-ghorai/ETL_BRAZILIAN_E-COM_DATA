import os
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import logging
from datetime import datetime

# Configure page layout
st.set_page_config(
    page_title="E-commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")

# Initialize BigQuery client
@st.cache_resource
def get_client():
    return bigquery.Client(project=PROJECT_ID)

client = get_client()

# Set up logging
log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app.log"))
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Function to execute BigQuery SQL and return results
@st.cache_data(ttl=3600)
def run_query(query):
    try:
        logger.info(f"Executing query: {query[:100]}...")
        query_job = client.query(query)
        results = query_job.result()
        return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        st.error(f"Failed to execute query: {e}")
        return []

# Header
st.title("📊 E-commerce Analytics Dashboard")
st.markdown("Analyze sales performance, customer distribution, and product reviews")

# Dashboard Tabs
tab1, tab2, tab3, tab4 = st.tabs(["Sales Overview", "Geographic Analysis", "Seller Performance", "Product Reviews"])

with tab1:
    st.header("📊 Sales Overview")

    # **Query: Total Revenue, Orders & YTD Sales**
    metrics_query = f"""
    SELECT 
        SUM(total_sales) AS total_revenue,
        SUM(total_orders) AS total_orders,
        SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(total_orders), 0)) AS avg_order_value,
        MAX(total_sales) AS best_month_sales
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_monthly_sales_trend`
    """

    # **Query: Monthly Growth Calculation**
    growth_query = f"""
    WITH ranked_months AS (
        SELECT 
            month,
            total_sales,
            ROW_NUMBER() OVER(ORDER BY month DESC) as row_num
        FROM `{PROJECT_ID}.{DATASET_ID}.Agg_monthly_sales_trend`
    )
    SELECT 
        latest.month as latest_month,
        latest.total_sales as latest_sales,
        previous.total_sales as previous_sales,
        SAFE_DIVIDE(latest.total_sales - previous.total_sales, NULLIF(previous.total_sales, 0)) * 100 AS growth_percentage
    FROM 
        (SELECT * FROM ranked_months WHERE row_num = 1) latest
    LEFT JOIN 
        (SELECT * FROM ranked_months WHERE row_num = 2) previous
    ON 1=1
    """

    with st.spinner("Loading metrics..."):
        metrics_results = run_query(metrics_query)
        growth_results = run_query(growth_query)

        if metrics_results:
            total_revenue = metrics_results[0]['total_revenue']
            total_orders = metrics_results[0]['total_orders']
            avg_order_value = metrics_results[0]['avg_order_value']
            best_month_sales = metrics_results[0]['best_month_sales']

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
            col2.metric("📦 Total Orders", f"{total_orders:,}")
            col3.metric("💵 Average Order Value", f"${avg_order_value:.2f}")
            col4.metric("🔥 Best Month Sales", f"${best_month_sales:,.2f}")

        # if growth_results:
            # monthly_growth = growth_results[0]['growth_percentage']
            # col4.metric("📈 Monthly Growth", f"{monthly_growth:.1f}%", delta=f"{monthly_growth:.1f}%")

    # **📌 Chart 1: Monthly Sales & Order Trends**
    # st.subheader("📆 Monthly Sales & Order Trends")

    monthly_trend_query = f"""
    SELECT 
        month,
        total_sales,
        total_orders
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_monthly_sales_trend`
    ORDER BY month
    """

    with st.spinner("Loading monthly trends..."):
        monthly_trend = run_query(monthly_trend_query)

        if monthly_trend:
            months = [row['month'] for row in monthly_trend]
            sales = [row['total_sales'] for row in monthly_trend]
            orders = [row['total_orders'] for row in monthly_trend]

            fig = go.Figure()

            # Total Sales Trend
            fig.add_trace(go.Scatter(
                x=months,
                y=sales,
                mode='lines+markers',
                name='Total Sales ($)',
                line=dict(color='royalblue', width=3)
            ))

            # Total Orders Trend
            fig.add_trace(go.Bar(
                x=months,
                y=orders,
                name='Total Orders',
                opacity=0.5,
                marker_color='orange',
                yaxis='y2'
            ))

            fig.update_layout(
                title="📊 Monthly Sales & Orders",
                xaxis=dict(title="Month"),
                yaxis=dict(title="Total Sales ($)"),
                yaxis2=dict(title="Total Orders", overlaying="y", side="right"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

    # **📌 Chart 2: Top Product Categories by Revenue**
    st.subheader("🏆 Best Selling Product Categories")

    category_query = f"""
    SELECT 
        category,
        SUM(total_sales) AS total_sales,
        SUM(total_orders) AS total_orders
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_sales_by_category`
    GROUP BY category
    ORDER BY total_sales DESC
    LIMIT 10
    """

    with st.spinner("Loading category data..."):
        category_data = run_query(category_query)

        if category_data:
            categories = [row['category'] for row in category_data]
            category_sales = [row['total_sales'] for row in category_data]
            category_orders = [row['total_orders'] for row in category_data]

            col1, col2 = st.columns(2)

            with col1:
                fig = go.Figure(go.Bar(
                    x=categories,
                    y=category_sales,
                    text=[f"${x:,.2f}" for x in category_sales],
                    textposition='auto',
                    marker_color='green'
                ))

                fig.update_layout(
                    title="💰 Top 10 Categories by Revenue",
                    xaxis=dict(title="Product Category", tickangle=-45),
                    yaxis=dict(title="Total Sales ($)"),
                    height=500
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = go.Figure(go.Bar(
                    x=categories,
                    y=category_orders,
                    text=[f"{x:,} orders" for x in category_orders],
                    textposition='auto',
                    marker_color='blue'
                ))

                fig.update_layout(
                    title="📦 Top 10 Categories by Orders",
                    xaxis=dict(title="Product Category", tickangle=-45),
                    yaxis=dict(title="Total Orders"),
                    height=500
                )

                st.plotly_chart(fig, use_container_width=True)

    # **🔍 Insights**
    highest_selling_category = categories[0]
    highest_orders_category = max(category_data, key=lambda x: x['total_orders'])['category']
    st.info(f"🏆 **{highest_selling_category}** has the highest revenue, while **{highest_orders_category}** has the most orders!")


with tab2:
    # st.header("🌎 Geographic Analysis")

    # **Query: State-wise Sales, Orders, AOV, and Unique Customers**
    state_query = f"""
    SELECT 
        customer_state,
        SUM(total_sales) AS total_sales,
        SUM(total_orders) AS total_orders,
        SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(total_orders), 0)) AS avg_order_value,
        COUNT(DISTINCT customer_state) AS unique_customers
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_sales_by_customer_state`
    GROUP BY customer_state
    ORDER BY total_sales DESC
    """

    with st.spinner("Loading geographic data..."):
        state_data = run_query(state_query)

        if state_data:
            st.subheader("📌 Sales Distribution by State")

            states = [row['customer_state'] for row in state_data]
            sales = [row['total_sales'] for row in state_data]
            orders = [row['total_orders'] for row in state_data]
            avg_order_value = [row['avg_order_value'] for row in state_data]

            # **📊 Chart 1: Top 10 States by Sales**
            fig = go.Figure(go.Bar(
                x=states[:10],
                y=sales[:10],
                text=[f"${x:,.2f}" for x in sales[:10]],
                textposition='auto',
                marker_color='blue'
            ))

            fig.update_layout(
                title="Top 10 States by Total Sales",
                xaxis=dict(title="State"),
                yaxis=dict(title="Total Sales ($)"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

            # **📌 Chart 2: State-wise Order Distribution**
            st.subheader("📦 Order Volume Across States")

            fig = go.Figure(go.Bar(
                x=states[:10],
                y=orders[:10],
                text=[f"{x:,.0f} orders" for x in orders[:10]],
                textposition='auto',
                marker_color='orange'
            ))

            fig.update_layout(
                title="Top 10 States by Order Volume",
                xaxis=dict(title="State"),
                yaxis=dict(title="Total Orders"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

            # **📌 Chart 3: Average Order Value Across States**
            st.subheader("💰 Average Order Value by State")

            fig = go.Figure(go.Bar(
                x=states[:10],
                y=avg_order_value[:10],
                text=[f"${x:,.2f}" for x in avg_order_value[:10]],
                textposition='auto',
                marker_color='green'
            ))

            fig.update_layout(
                title="States with the Highest AOV",
                xaxis=dict(title="State"),
                yaxis=dict(title="Average Order Value ($)"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

            # **🔍 Insights**
            highest_sales_state = states[0]
            highest_aov_state = max(state_data, key=lambda x: x['avg_order_value'])['customer_state']
            st.info(f"🔥 **{highest_sales_state}** has the highest total sales, but **{highest_aov_state}** has the highest Average Order Value!")


with tab3:
    # st.header("🏆 Seller Performance")

    # **Query 1: Top Sellers by Revenue & Orders**
    sellers_query = f"""
    SELECT 
        seller_id,
        seller_state,
        SUM(total_sales) AS total_sales,
        SUM(total_orders) AS total_orders,
        SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(total_orders), 0)) AS avg_order_value
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_sales_by_seller`
    GROUP BY seller_id, seller_state
    ORDER BY total_sales DESC
    LIMIT 10
    """

    # **Query 2: Seller Distribution by State**
    seller_state_query = f"""
    SELECT 
        seller_state,
        SUM(total_sales) AS total_sales,
        COUNT(DISTINCT seller_id) AS total_sellers,
        SUM(total_orders) AS total_orders
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_sales_by_seller`
    GROUP BY seller_state
    ORDER BY total_sales DESC
    """

    # **Query 3: Seller Efficiency (Average Order Value)**
    efficiency_query = f"""
    SELECT 
        seller_id,
        seller_state,
        SUM(total_sales) AS total_sales,
        SUM(total_orders) AS total_orders,
        SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(total_orders), 0)) AS avg_order_value
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_sales_by_seller`
    GROUP BY seller_id, seller_state
    ORDER BY avg_order_value DESC
    LIMIT 15
    """

    with st.spinner("Loading seller data..."):
        top_sellers = run_query(sellers_query)
        seller_by_state = run_query(seller_state_query)
        efficiency_data = run_query(efficiency_query)

        # **📌 Chart 1: Top 10 Sellers by Revenue & Orders**
        if top_sellers:
            st.subheader("💰 Top 10 Sellers by Revenue & Orders")

            seller_ids = [row['seller_id'] for row in top_sellers]
            seller_sales = [row['total_sales'] for row in top_sellers]
            seller_orders = [row['total_orders'] for row in top_sellers]

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=seller_ids,
                y=seller_sales,
                name="Total Sales ($)",
                marker_color='blue',
                text=[f"${x:,.2f}" for x in seller_sales],
                textposition='auto'
            ))

            fig.add_trace(go.Scatter(
                x=seller_ids,
                y=seller_orders,
                mode='lines+markers',
                name="Total Orders",
                yaxis="y2",
                marker=dict(color='red', size=8)
            ))

            fig.update_layout(
                title="Top Sellers: Revenue vs Orders",
                xaxis=dict(title='Seller ID', tickangle=-45),
                yaxis=dict(title="Total Sales ($)"),
                yaxis2=dict(title="Total Orders", overlaying="y", side="right"),
                legend=dict(orientation="h"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

            # **🔍 Insight:**
            best_seller = seller_ids[0]
            st.info(f"🚀 **{best_seller}** is the top seller by revenue! But does it have the most orders?")

        # **📌 Chart 2: Seller Distribution by State**
        if seller_by_state:
            st.subheader("📍 Where Are the Best Sellers Located?")

            states = [row['seller_state'] for row in seller_by_state]
            total_sellers = [row['total_sellers'] for row in seller_by_state]
            state_sales = [row['total_sales'] for row in seller_by_state]

            col1, col2 = st.columns(2)

            with col1:
                fig = go.Figure(go.Bar(
                    x=states,
                    y=total_sellers,
                    text=[f"{x} sellers" for x in total_sellers],
                    textposition='auto',
                    marker_color='purple'
                ))

                fig.update_layout(
                    title="Number of Sellers per State",
                    xaxis=dict(title="State"),
                    yaxis=dict(title="Total Sellers"),
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = go.Figure(go.Bar(
                    x=states,
                    y=state_sales,
                    text=[f"${x:,.2f}" for x in state_sales],
                    textposition='auto',
                    marker_color='green'
                ))

                fig.update_layout(
                    title="Total Sales by Seller State",
                    xaxis=dict(title="State"),
                    yaxis=dict(title="Total Sales ($)"),
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

            # **🔍 Insight:**
            top_seller_state = states[0]
            st.info(f"📍 **{top_seller_state}** has the most sellers! But does it have the highest revenue per seller?")

        # **📌 Chart 3: Seller Efficiency (AOV Bubble Chart)**
        if efficiency_data:
            st.subheader("📊 Who Are the Most Efficient Sellers?")

            fig = go.Figure(go.Scatter(
                x=[row['total_orders'] for row in efficiency_data],
                y=[row['total_sales'] for row in efficiency_data],
                mode='markers',
                marker=dict(
                    size=[max(5, min(row['avg_order_value'] / 10, 50)) for row in efficiency_data],  # Adjust marker size
                    color=[row['avg_order_value'] for row in efficiency_data],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Avg Order Value ($)")
                ),
                text=[f"Seller: {row['seller_id']}<br>State: {row['seller_state']}<br>AOV: ${row['avg_order_value']:.2f}<br>Orders: {row['total_orders']}" for row in efficiency_data]
            ))

            fig.update_layout(
                title="Seller Efficiency: Revenue vs Orders",
                xaxis=dict(title="Number of Orders"),
                yaxis=dict(title="Total Sales ($)"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

            # **🔍 Insight:**
            most_efficient_seller = max(efficiency_data, key=lambda x: x['avg_order_value'])['seller_id']
            st.info(f"💎 **{most_efficient_seller}** has the highest AOV! This means fewer orders but high-value sales.")



with tab4:
    # st.header("📢 Product Reviews Analysis")

    # **Query 1: Get Top 10 Categories by Total Reviews**
    review_count_query = f"""
    SELECT 
        category,
        SUM(total_reviews) as total_reviews,
        SAFE_DIVIDE(SUM(total_reviews * avg_review_score), NULLIF(SUM(total_reviews), 0)) as avg_review_score
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_avg_review_score`
    GROUP BY category
    ORDER BY total_reviews DESC
    LIMIT 10
    """

    # **Query 2: Get Relationship Between Reviews & Sales**
    review_sales_correlation_query = f"""
    SELECT 
        r.category,
        SUM(r.total_reviews) as total_reviews,
        SAFE_DIVIDE(SUM(r.total_reviews * r.avg_review_score), NULLIF(SUM(r.total_reviews), 0)) as avg_review_score,
        SUM(s.total_sales) as total_sales
    FROM `{PROJECT_ID}.{DATASET_ID}.Agg_avg_review_score` r
    JOIN `{PROJECT_ID}.{DATASET_ID}.Agg_sales_by_category` s
    ON r.category = s.category
    GROUP BY r.category
    ORDER BY total_sales DESC
    LIMIT 15
    """

    with st.spinner("Loading review data..."):
        top_reviewed_categories = run_query(review_count_query)
        review_sales_data = run_query(review_sales_correlation_query)

        # **📌 Chart 1: Top 10 Categories by Total Reviews**
        if top_reviewed_categories:
            st.subheader("📊 Top 10 Categories by Total Reviews")

            categories = [row['category'] for row in top_reviewed_categories]
            total_reviews = [row['total_reviews'] for row in top_reviewed_categories]
            avg_scores = [row['avg_review_score'] for row in top_reviewed_categories]

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=categories,
                y=total_reviews,
                text=[f"{x:,.0f} reviews" for x in total_reviews],
                textposition='auto',
                marker_color='blue',
                name="Total Reviews"
            ))

            fig.add_trace(go.Scatter(
                x=categories,
                y=avg_scores,
                mode='lines+markers',
                name="Avg Review Score",
                yaxis="y2",
                marker=dict(color='red', size=8)
            ))

            fig.update_layout(
                title="Most Reviewed Categories",
                xaxis=dict(title="Product Category", tickangle=-45),
                yaxis=dict(title="Total Reviews"),
                yaxis2=dict(title="Avg Review Score", overlaying="y", side="right", range=[1, 5]),
                legend=dict(orientation="h"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

            # **🔍 Insight:**
            most_reviewed_category = categories[0]
            st.info(f"🔥 **{most_reviewed_category}** has the highest number of reviews, indicating strong customer engagement!")

        # **📌 Chart 2: Relationship Between Reviews & Sales**
        if review_sales_data:
            st.subheader("📈 Do More Reviews Mean Higher Sales?")

            fig = go.Figure(go.Scatter(
                x=[row['total_reviews'] for row in review_sales_data],
                y=[row['total_sales'] for row in review_sales_data],
                mode='markers',
                marker=dict(
                    size=[max(5, min(row['avg_review_score'] * 8, 50)) for row in review_sales_data],
                    color=[row['avg_review_score'] for row in review_sales_data],
                    colorscale='Blues',
                    cmin=1,
                    cmax=5,
                    showscale=True,
                    colorbar=dict(title='Avg Review Score')
                ),
                text=[f"Category: {row['category']}<br>Reviews: {row['total_reviews']}<br>Avg Score: {row['avg_review_score']:.2f}<br>Sales: ${row['total_sales']:,.2f}" for row in review_sales_data]
            ))

            fig.update_layout(
                title="Correlation Between Review Volume & Sales",
                xaxis=dict(title="Total Reviews"),
                yaxis=dict(title="Total Sales ($)"),
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)

            # **🔍 Insight:**
            highest_sales_category = max(review_sales_data, key=lambda x: x['total_sales'])['category']
            highest_review_category = max(review_sales_data, key=lambda x: x['total_reviews'])['category']

            st.info(f"💡 **{highest_sales_category}** has the highest sales, but **{highest_review_category}** has the most reviews. More reviews = more sales!")
