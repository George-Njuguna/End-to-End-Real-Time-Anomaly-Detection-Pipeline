from log_mlflow import load_production_model
from functions import split_func, create_prediction_table, import_data, load_data
import psycopg2
import pandas as pd
import os 


tracking_uri = "http://mlflow:5001"
new_table_name = "predictions"
model_name = "fraud_detection_test"
table_name1 = "streaming_data"
table_name2 = "transactions"

 # Connecting to database 
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PW'),
    host = os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)

test_data = import_data( table_name1, conn )

model, run_id, version = load_production_model(model_name, tracking_uri)

column = test_data['time_seconds']

test_data, y = split_func(test_data)

predictions = model.predict(test_data)
probabilities = model.predict_proba(test_data)[:, 1] * 100

 # appending the true fraud
test_data['fraud'] = y
test_data['time_seconds'] = column

# loading to the transaction table for future remodelling 
#load_data(conn, test_data, table_name2) 

# Append predictions
test_data["prediction"] = predictions
test_data["probability"] = probabilities

# loading the data
create_prediction_table(conn, new_table_name)
#load_data(conn, test_data, new_table_name)


def main():
    print("END OF INFERENCE ")
    
if __name__ == '__main__':
    main()