import os
import pandas as pd
import numpy as np  # Import NumPy for NaN handling
import mysql.connector

# MySQL Connection Details
DB_USERNAME = "root"
DB_PASSWORD = "Suman@2717"
DB_HOST = "localhost"
DB_PORT = 3306
DB_NAME = "e-com"

# Establish MySQL Connection
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=DB_PORT
)

cursor = conn.cursor()

# Load CSV File
csv_file = "data/olist_customers_dataset.csv"  # Change this to your actual CSV file path
table_name = os.path.splitext(os.path.basename(csv_file))[0]  # Extract filename without extension

# Read the CSV into a DataFrame
df = pd.read_csv(csv_file)

# Convert column names to valid MySQL column names (replace "-" and spaces with "_")
df.columns = [col.replace("-", "_").replace(" ", "_") for col in df.columns]

# Replace NaN values with None (MySQL NULL)
df = df.replace({pd.NA: None, np.nan: None})

# Create Table Dynamically Based on CSV Headers
create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
create_table_query += "id INT AUTO_INCREMENT PRIMARY KEY,\n"  # Auto-increment ID
for column in df.columns:
    create_table_query += f"`{column}` TEXT,\n"  # Using TEXT to handle different data types
create_table_query = create_table_query.rstrip(",\n") + "\n);"

# Execute Table Creation Query
cursor.execute(create_table_query)

# Prepare Insert Query Dynamically
columns = ", ".join([f"`{col}`" for col in df.columns])
placeholders = ", ".join(["%s"] * len(df.columns))
insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

# Upload Data to MySQL
for _, row in df.iterrows():
    cursor.execute(insert_query, tuple(row.replace({pd.NA: None, np.nan: None})))  # Ensures no NaN values are passed

# Commit and Close Connection
conn.commit()
cursor.close()
conn.close()

print(f"Table `{table_name}` created (if missing) and data uploaded successfully!")
