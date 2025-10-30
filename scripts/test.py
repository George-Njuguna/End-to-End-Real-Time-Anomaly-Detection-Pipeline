from log_mlflow import load_production_model
from functions import import_data
import psycopg2
import pandas as pd
import os 


tracking_uri = "http://mlflow:5001"
model_name = "fraud_detection_test"
table_name = "streaming_data"

 # Connecting to database 
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PW'),
    host = os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)

test_data = import_data( table_name, conn )

model, run_id, version = load_production_model(model_name, tracking_uri)

predictions = model(test_data)

# Append predictions
test_data["prediction"] = predictions

print(test_data.head(10))

def main():
    print("END OF INFERENCE ")
    
if __name__ == '__main__':
    main()