from google.cloud import bigquery
import streamlit as st

def upload_to_bigquery(df, table_name, project_id, dataset_id, partition_col=None):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    dataset_ref = client.dataset(dataset_id)

    # Check if the table exists
    try:
        client.get_table(table_ref)
        st.info(f"Table {table_name} already exists. Proceeding with data upload...")
    except:
        st.warning(f"Table {table_name} not found. Creating a new partitioned table...")

        # Define schema from DataFrame
        schema = [
            bigquery.SchemaField(col, "STRING") if df[col].dtype == "object" else
            bigquery.SchemaField(col, "FLOAT64") if df[col].dtype == "float64" else
            bigquery.SchemaField(col, "INTEGER") if df[col].dtype == "int64" else
            bigquery.SchemaField(col, "TIMESTAMP") if col == partition_col else
            bigquery.SchemaField(col, "STRING")  # Default fallback
            for col in df.columns
        ]

        # Define table configuration with partitioning
        table = bigquery.Table(table_ref, schema=schema)
        
        if partition_col:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_col
            )

        # Create table in BigQuery
        client.create_table(table)
        st.success(f"Created partitioned table {table_name} in BigQuery.")

    # Define Job Config
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    # Load Data
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        st.success(f"Uploaded {table_name} to BigQuery.")
    except Exception as e:
        st.error(f"Error uploading {table_name}: {e}")
