import kaggle
import pandas as pd
import kagglehub
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()

db_uri = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"


kaggle.api.authenticate()  

# Download latest version
path = kagglehub.dataset_download("smayanj/e-commerce-transactions-dataset")

print("Path to dataset files:", path)
print("Database URI:", db_uri)

def etl_pipeline():
    # Extract
    df = pd.read_csv(f"{path}/ecommerce_transactions.csv")
    
    # Transform
    df.columns = [col.replace(" ", "_").lower() for col in df.columns]
    df['purchase_amount'] = df['purchase_amount'].fillna(df['purchase_amount'].mean())
    df = df.dropna()
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
    #Load
    engine = create_engine(db_uri)
    df.to_sql("transactions", engine, if_exists="append", index=False)
    
    print("ETL process completed successfully!")
  
print(etl_pipeline())