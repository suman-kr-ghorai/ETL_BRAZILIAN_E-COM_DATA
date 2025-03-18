import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\suman\AppData\Roaming\gcloud\application_default_credentials.json"

# project_id = "brazilian-ecom"
# dataset_id = "test2"

# Set up BigQuery Client


# Function to push DataFrame to BigQuery
def upload_to_bigquery(df, table_name,project_id,dataset_id):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    
    # Load DataFrame into BigQuery
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait for completion
    print(f"Uploaded {table_name} to BigQuery.")

# Upload DataFrames


    # print("All tables uploaded successfully!")
