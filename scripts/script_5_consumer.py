from log_mlflow import load_production_model
from kafka import KafkaConsumer
import json
import psycopg2
import pandas as pd
import os 

tracking_uri = "http://mlflow:5001"
model_name = "fraud_detection_test"
table_name = "infered_transactions"

 # Connecting to database 
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PW'),
    host = os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)

model, run_id, version = load_production_model(model_name, tracking_uri)