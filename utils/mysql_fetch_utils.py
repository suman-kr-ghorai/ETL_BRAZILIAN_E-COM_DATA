from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.engine.url import URL

def fetch_table(table_name):
    
    
    # Create MySQL connection using SQLAlchemy
    db_url = URL.create(
        drivername="mysql+pymysql",
        username="root",
        password="Suman@2717",  # Consider using environment variables for security
        host="localhost",
        database="e-com"
    )
    
    engine = create_engine(db_url)
    
    # Fetch table into DataFrame
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)

    df=df.drop(columns=['id'])

    return df


