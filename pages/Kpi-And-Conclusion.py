import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import os
from datetime import datetime
import altair as alt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from pathlib import Path
import pptx
from pptx import Presentation
from pptx.util import Inches, Pt
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import base64
import io

# Create output directories if they don't exist
os.makedirs("visualizations", exist_ok=True)
os.makedirs("reports", exist_ok=True)

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

# Function to save Plotly figure as image
def save_figure(fig, filename):
    """Save a Plotly figure as an image file"""
    path = os.path.join("visualizations", filename)
    fig.write_image(path, scale=2)
    return path


# Create PDF report
def create_pdf(visualizations, insights):
    """Create a PDF report with visualizations and insights"""
    report_path = os.path.join("reports", f"ecommerce_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
    doc = SimpleDocTemplate(report_path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []
    
    # Title
    title_style = styles["Title"]
    elements.append(Paragraph("Brazilian E-commerce Analytics Report", title_style))
    elements.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d')}", styles["Heading2"]))
    elements.append(Spacer(1, 0.25*inch))
    
    # Executive Summary
    elements.append(Paragraph("Executive Summary", styles["Heading1"]))
    summary_text = """
    The Brazilian e-commerce platform demonstrates strong revenue growth and high order fulfillment rates.
    Customer segmentation reveals that a small percentage of high-value customers drive a disproportionate share of revenue.
    Seller distribution shows concentration in major urban centers, with São Paulo dominating the market.
    Product category performance varies significantly, indicating opportunities for targeted strategies.
    Geographic performance suggests potential for regional expansion initiatives.
    """
    elements.append(Paragraph(summary_text, styles["Normal"]))
    elements.append(Spacer(1, 0.25*inch))
    
    # Add visualizations and insights
    for viz_name, viz_path in visualizations.items():
        elements.append(Paragraph(viz_name, styles["Heading2"]))
        elements.append(Spacer(1, 0.1*inch))
        elements.append(Image(viz_path, width=6*inch, height=4*inch))
        elements.append(Spacer(1, 0.1*inch))
        
        # Find matching insights
        for section, section_insights in insights.items():
            if section.lower() in viz_name.lower() or viz_name.lower() in section.lower():
                elements.append(Paragraph("Key Insights:", styles["Heading3"]))
                elements.append(Paragraph(section_insights, styles["Normal"]))
                elements.append(Spacer(1, 0.25*inch))
                break
    
    # Build PDF
    doc.build(elements)
    return report_path

# Function to send email with report attachment
def send_email(recipients, subject, body, attachment_path):
    """Send an email with the report attached to multiple recipients"""

    sender_email = os.environ.get("EMAIL_SENDER", "")  # Get from environment variable
    password = os.environ.get("EMAIL_PASSWORD", "")    # Get from environment variable
    
    if not sender_email or not password:
        return False, "Email configuration missing. Please set EMAIL_SENDER and EMAIL_PASSWORD environment variables."
    
    # Convert recipients string into a list
    recipient_list = [email.strip() for email in recipients.split(",") if email.strip()]

    # Create message
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipient_list)  # Set multiple recipients
    msg["Subject"] = subject
    
    # Attach body text
    msg.attach(MIMEText(body, "plain"))
    
    # Attach file
    with open(attachment_path, "rb") as file:
        attachment = MIMEApplication(file.read(), Name=os.path.basename(attachment_path))
        attachment["Content-Disposition"] = f'attachment; filename="{os.path.basename(attachment_path)}"'
        msg.attach(attachment)
    
    try:
        # Connect to Gmail SMTP server
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, password)
        
        # Send email
        server.sendmail(sender_email, recipient_list, msg.as_string())
        server.quit()
        return True, "Email sent successfully!"
    except Exception as e:
        return False, f"Failed to send email: {str(e)}"


# Dashboard title
st.title("🛍️ Brazilian E-commerce Analytics Dashboard")
st.markdown("This dashboard presents key performance indicators from the Brazilian e-commerce dataset.")

# Create tabs for different analysis sections
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Orders & Revenue", 
    "Customer Analysis", 
    "Seller Analysis",
    "Year-over-Year Trends",
    "Category Analysis",
    "Reports & Email"
])

# Dictionary to store all visualizations and insights
all_visualizations = {}
all_insights = {}

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
            
            # Save figure
            fig_path = save_figure(fig, "orders_by_status.png")
            all_visualizations["Orders by Status"] = fig_path
            
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
            
            # Save figure
            fig_path = save_figure(fig, "revenue_by_status.png")
            all_visualizations["Revenue by Order Status"] = fig_path
        
        # Insights
        st.subheader("Insights")
        orders_insights = """
        The majority of orders are in 'delivered' status, indicating a healthy fulfillment rate.
        Cancelled orders represent a small percentage of total orders, suggesting good order qualification.
        The average order value varies by status, with 'processing' orders showing higher values, possibly indicating larger purchases take longer to process.
        Delivered orders generate the bulk of revenue, as expected.
        The revenue lost from cancelled orders represents an opportunity for recovery strategies.
        """
        st.markdown(orders_insights)
        
        # Store insights
        all_insights["Orders and Revenue"] = orders_insights
        
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
            
            # Save figure
            fig_path = save_figure(fig, "customer_segmentation.png")
            all_visualizations["Customer Segmentation"] = fig_path
            
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
            
            # Save figure
            fig_path = save_figure(fig, "revenue_by_customer_segment.png")
            all_visualizations["Revenue by Customer Segment"] = fig_path
        
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
        
        # Save figure
        fig_path = save_figure(fig, "order_frequency.png")
        all_visualizations["Order Frequency Analysis"] = fig_path
        
        # Insights
        st.subheader("Insights")
        customer_insights = """
        The majority of customers make only 1-2 purchases, indicating an opportunity for retention strategies.
        Premium customers (top 25% by spending) contribute disproportionately to total revenue.
        There's significant potential to move customers from Medium to High Value segments with targeted marketing.
        Implementing a loyalty program could increase repeat purchases.
        Targeted offers for Medium Value customers could increase their average order value.
        Specialized retention campaigns for High Value and Premium customers are recommended.
        """
        st.markdown(customer_insights)
        
        # Store insights
        all_insights["Customer Analysis"] = customer_insights
        
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
        
        # Save figure
        fig_path = save_figure(fig, "top_cities_revenue.png")
        all_visualizations["Top Cities by Revenue"] = fig_path
        
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
            
            # Save figure
            fig_path = save_figure(fig, "seller_segmentation.png")
            all_visualizations["Seller Segmentation"] = fig_path
            
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
            
            # Save figure
            fig_path = save_figure(fig, "revenue_by_seller_segment.png")
            all_visualizations["Revenue by Seller Segment"] = fig_path
        
        # Insights
        st.subheader("Insights")
        seller_insights = """
        A small number of Enterprise sellers (top 5%) generate a substantial portion of total revenue.
        Sellers are concentrated in major urban centers, with São Paulo being the dominant market.
        The long tail of Small sellers represents an opportunity for targeted growth programs.
        Major cities have a higher seller-to-revenue ratio, indicating stronger market efficiency.
        Smaller cities show potential for expansion with the right support and incentives.
        Regional differences in seller performance suggest the need for localized strategies.
        """
        st.markdown(seller_insights)
        
        # Store insights
        all_insights["Seller Analysis"] = seller_insights
        
    except Exception as e:
        st.error(f"Error loading seller data: {e}")

# Tab 4: Year-over-Year Trends
with tab4:
    st.header("Year-over-Year Performance Analysis")
    
    try:
        # Load data
        yoy_revenue_df = load_yoy_revenue()
        yoy_city_df = load_yoy_seller_city()
        
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
        
        # Save figure
        fig_path = save_figure(fig, "annual_revenue_trend.png")
        all_visualizations["Annual Revenue Trend"] = fig_path
        
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
        
        category_colors = ['#FF4B4B', '#3366FF', '#33CC33', '#FF33CC', '#FFCC00']
        
        fig = px.line(
            top_cities_data,
            x='year',
            y='total_revenue',
            color='seller_city',
            title='Top 5 Cities - Revenue Trend',
            markers=True,
            line_shape='linear',
            color_discrete_sequence=category_colors  # Use bright colors
        )
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Total Revenue (R$)",
            xaxis=dict(tickmode='linear')
        )
        fig.update_traces(line=dict(width=3), marker=dict(size=10))  # Make lines thicker for better visibility
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Save figure
        fig_path = save_figure(fig, "city_performance_by_year.png")
        all_visualizations["City Performance by Year"] = fig_path
        
        # Insights
        st.subheader("Insights")
        yoy_insights = """
        The platform shows consistent revenue growth year over year, indicating market expansion.
        Growth rates vary by year, with more recent periods showing accelerated growth.
        Major cities maintain their dominant position, but some secondary markets are growing faster.
        The growth trajectory suggests a maturing market with increasing e-commerce adoption.
        Year-over-year variations highlight the impact of economic factors and platform developments.
        City-specific trends reveal opportunities for targeted regional expansion strategies.
        """
        st.markdown(yoy_insights)
        
        # Store insights
        all_insights["Year-over-Year Trends"] = yoy_insights
        
    except Exception as e:
        st.error(f"Error loading YoY data: {e}")

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
            color_continuous_scale='Turbo'  # Use Turbo color scale for more vivid colors
        )
        fig.update_layout(
            xaxis_title="Product Category",
            yaxis_title="Total Revenue (R$)",
            xaxis={'categoryorder':'total descending'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Save figure
        fig_path = save_figure(fig, "top_categories_revenue.png")
        all_visualizations["Top Categories by Revenue"] = fig_path
        
        # YoY Category Trends
        st.subheader("Category Trends Over Time")
        
        try:
            # Try to get YoY category data
            yoy_category_df = load_yoy_product_category()
            
            # Get top 5 categories
            top_cats = yoy_category_df.groupby('product_category')['total_revenue'].sum().nlargest(5).index.tolist()
            
            # Filter for top categories
            top_cats_data = yoy_category_df[yoy_category_df['product_category'].isin(top_cats)]
            
            # Use a high-contrast color palette for better visibility in PDF
            category_colors = ['#FF4B4B', '#3366FF', '#33CC33', '#FF33CC', '#FFCC00']
            
            fig = px.line(
                top_cats_data,
                x='year',
                y='total_revenue',
                color='product_category',
                title='Top 5 Categories - Revenue Trend',
                markers=True,
                line_shape='linear',
                color_discrete_sequence=category_colors  # Use bright colors
            )
            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Total Revenue (R$)",
                xaxis=dict(tickmode='linear')
            )
            # Make lines thicker for better visibility
            fig.update_traces(line=dict(width=3), marker=dict(size=10))
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Save figure
            fig_path = save_figure(fig, "category_trends_over_time.png")
            all_visualizations["Category Trends Over Time"] = fig_path
            
        except Exception as e:
            st.warning(f"Unable to load category trend data: {e}")
        
        # Insights
        st.subheader("Insights")
        category_insights = """
        - The top categories represent a significant portion of overall revenue, indicating category concentration.
        - Categories show different growth patterns, with some emerging categories showing rapid expansion.
        - Traditional categories maintain stable revenue streams, while trending categories show more volatility.
        - Invest in expanding inventory and seller recruitment in high-growth categories.
        - Consider specialized marketing campaigns for seasonal category performance.
        - Monitor emerging categories for early identification of consumer trends.
        """
        st.markdown(category_insights)
        
        # Store insights
        all_insights["Category Analysis"] = category_insights
        
    except Exception as e:
        st.error(f"Error loading category data: {e}")


# Tab 6: Reports & Email

with tab6:
    st.header("Generate and Send Reports")
    
    # Report format selection
    report_format = st.radio(
        "Select Report Format:",
        ["PDF"]
    )
    
    # Generate Report button
    if st.button("Generate Report"):
        with st.spinner("Generating report..."):
            try:
                if report_format == "PDF":
                    report_path = create_pdf(all_visualizations, all_insights)
                else:  # PowerPoint
                    # report_path = create_powerpoint(all_visualizations, all_insights)
                    pass
                
                st.success(f"{report_format} report generated successfully!")

                # Create download link
                with open(report_path, "rb") as file:
                    report_data = file.read()
                    b64_report = base64.b64encode(report_data).decode()
                
                filename = os.path.basename(report_path)
                href = f'<a href="data:application/octet-stream;base64,{b64_report}" download="{filename}">Download {report_format} Report</a>'
                st.markdown(href, unsafe_allow_html=True)
                
                # Store the path in session state so it's available outside this button click
                st.session_state.report_path = report_path
                
            except Exception as e:
                st.error(f"Error generating report: {str(e)}")

    # Email form - only show after a report has been generated
    if 'report_path' in st.session_state:
        st.subheader("Send Report via Email")

        # Input multiple email addresses (comma-separated)
        recipient_emails = st.text_input("Recipient Email Addresses (comma-separated)")
        email_subject = st.text_input("Email Subject", f"Brazilian E-commerce Analytics Report - {datetime.now().strftime('%Y-%m-%d')}")
        email_body = st.text_area("Email Message", 
                                f"Hello,\n\nPlease find attached the Brazilian E-commerce Analytics Report generated on {datetime.now().strftime('%Y-%m-%d')}.\n\nRegards,\nE-commerce Analytics Team")
        
        # Send email button
        if st.button("Send Email"):
            if not recipient_emails:
                st.error("Please enter at least one recipient email address.")
            else:
                with st.spinner("Sending email..."):
                    success, message = send_email(recipient_emails, email_subject, email_body, st.session_state.report_path)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
    else:
        st.info("Please generate a report first before sending an email.")
