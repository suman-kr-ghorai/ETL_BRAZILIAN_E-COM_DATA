from google.cloud import bigquery
import os

def create_kpi_tables(project_id,dataset_id):
    """
    Creates KPI tables in BigQuery based on predefined queries.
    """
    
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define KPI table queries
    kpi_queries = {
        "KPI_Sales_Overview": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.KPI_Sales_Overview` AS
            SELECT 
                SUM(total_sales) AS total_sales,
                SUM(total_orders) AS total_orders,
                SUM(total_sales) / NULLIF(SUM(total_orders), 0) AS avg_sales_per_order
            FROM `{project_id}.{dataset_id}.Agg_sales_by_category`;
        """,
        "KPI_Customer_Insights": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.KPI_Customer_Insights` AS
            SELECT 
                COUNT(DISTINCT customer_state) AS unique_customer_states,
                SUM(total_sales) AS total_sales,
                SUM(total_sales) / NULLIF(COUNT(unique_customers), 0) AS avg_spend_per_customer
            FROM `{project_id}.{dataset_id}.Agg_sales_by_customer_state`;
        """,
        "KPI_Seller_Performance": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.KPI_Seller_Performance` AS
            SELECT 
                seller_id,
                SUM(total_sales) AS total_sales,
                SUM(total_orders) AS total_orders,
                SUM(total_sales) / NULLIF(SUM(total_orders), 0) AS avg_order_value
            FROM `{project_id}.{dataset_id}.Agg_sales_by_seller`
            GROUP BY seller_id;
        """,
        "KPI_Product_Review_Scores": f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.KPI_Product_Review_Scores` AS
            SELECT 
                category,
                AVG(avg_review_score) AS avg_review_score,
                SUM(total_reviews) AS total_reviews
            FROM `{project_id}.{dataset_id}.Agg_avg_review_score`
            GROUP BY category;
        """
    }
    
    # Execute Queries
    for kpi, query in kpi_queries.items():
        job = client.query(query)
        job.result()  # Wait for execution to complete
        print(f"Table {kpi} created successfully.")

# Example usage
if __name__ == "__main__":
    create_kpi_tables()
