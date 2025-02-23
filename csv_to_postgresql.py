import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection configuration
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"
TABLE_NAME = "customers"

def load_csv_to_postgres(csv_file: str, table_name: str, if_exists: str = "replace"):
    try:
        # Read data from the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_file)

        # Create a connection to PostgreSQL using SQLAlchemy
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

        # Write the DataFrame to PostgreSQL
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)

        print(f"Data from '{csv_file}' has been successfully inserted into the '{table_name}' table in PostgreSQL.")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    csv_file_path = "customers.csv"
    load_csv_to_postgres(csv_file_path, TABLE_NAME)
