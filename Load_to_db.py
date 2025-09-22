 # Importing libraries
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from functions import feat_eng, split, create_train_table, create_test_table, load_test_data,load_train_data

load_dotenv()

 # Loading the CSV FILE 
try:
    DATA = pd.read_csv('creditcard.csv')
except Exception as e:
    print(" ERROR IN READING CSV FILE:", e)

 # feature engineering
try:
    data = feat_eng(DATA)
except Exception as e:
    print(" ERROR IN [ADDING TIMESTAMP AND CHANGING COLUMN NAMES]:", e)


 # splitting the data
try:
    train_df, test_df = split(data)
except Exception as e:
    print(" ERROR IN [SPLITTING THE DATASETS]:", e)

 # LOADING THE DATA TO POSTGRESS
try:
    # connecting to the database
    conn = psycopg2.connect(
        dbname=os.getenv('database'),
        user=os.getenv('user'),
        password=os.getenv('password'),
        host = os.getenv('host'),
        port=os.getenv('port')
    )
    print('✅ Connection made')

    # Checking if the tables Exist/Creating The Tables
    create_train_table(conn)
    create_test_table(conn)
    
    # Loading The Data
    load_test_data(conn, test_df)
    load_train_data(conn, train_df)

except Exception as e:
    print("❌ ERROR:", e)
    if conn:
        conn.rollback()
finally:
    if conn:
        conn.close()
        print("🔌 CONNECTION CLOSED")

def main():
    print("✅✅ COMPLETED LOADING THE DATASET TO POSTGRESS")
    
if __name__ == '__main__':
    main()