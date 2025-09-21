 # Libraries
import pandas as pd
from sklearn.model_selection import train_test_split

 # Creating Timestamp Column
def feat_eng( data ):
    start_time = pd.to_datetime("2025-09-20 00:00:00")
    data["Timestamp"] = start_time + pd.to_timedelta( data["Time"], unit="s" )
    data["Time"] = data['Time'].astype(int)
    data = data.rename( columns = {'Time' : 'Time_elapsed_sec' , 'Class' : 'Fraud'} )
    return data


 # splitting the dataset into train and test 
def split(data):
    train_df, test_df = train_test_split(
        data,
        test_size=0.2,           
        random_state=42,
        stratify=data["Fraud"]
    )
    return train_df , test_df


 # Creating tables in postgress
 # transactions_train_raw
def create_train_table( conn ):   
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions_train_raw (
                    transaction_id SERIAL PRIMARY KEY,     
                    time_seconds INT NOT NULL,              
                    timestamp TIMESTAMP NOT NULL,           
                    amount NUMERIC(10,2) NOT NULL,          
                    v1 DOUBLE PRECISION,
                    v2 DOUBLE PRECISION,
                    v3 DOUBLE PRECISION,
                    v4 DOUBLE PRECISION,
                    v5 DOUBLE PRECISION,
                    v6 DOUBLE PRECISION,
                    v7 DOUBLE PRECISION,
                    v8 DOUBLE PRECISION,
                    v9 DOUBLE PRECISION,
                    v10 DOUBLE PRECISION,
                    v11 DOUBLE PRECISION,
                    v12 DOUBLE PRECISION,
                    v13 DOUBLE PRECISION,
                    v14 DOUBLE PRECISION,
                    v15 DOUBLE PRECISION,
                    v16 DOUBLE PRECISION,
                    v17 DOUBLE PRECISION,
                    v18 DOUBLE PRECISION,
                    v19 DOUBLE PRECISION,
                    v20 DOUBLE PRECISION,
                    v21 DOUBLE PRECISION,
                    v22 DOUBLE PRECISION,
                    v23 DOUBLE PRECISION,
                    v24 DOUBLE PRECISION,
                    v25 DOUBLE PRECISION,
                    v26 DOUBLE PRECISION,
                    v27 DOUBLE PRECISION,
                    v28 DOUBLE PRECISION,
                    fraud SMALLINT NOT NULL

                );
            """)
            conn.commit()
            print("✅ Table 'transactions_train_raw' CREATED/EXISTS).")
    
    except Exception as e:
        print("❌ ERROR Creating table transactions_train_raw : ", e)
        if conn:
            conn.rollback()


 # transactions_test_raw            
def create_test_table( conn ):   
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions_test_raw (
                    transaction_id SERIAL PRIMARY KEY,     
                    time_seconds INT NOT NULL,              
                    timestamp TIMESTAMP NOT NULL,           
                    amount NUMERIC(10,2) NOT NULL,          
                    v1 DOUBLE PRECISION,
                    v2 DOUBLE PRECISION,
                    v3 DOUBLE PRECISION,
                    v4 DOUBLE PRECISION,
                    v5 DOUBLE PRECISION,
                    v6 DOUBLE PRECISION,
                    v7 DOUBLE PRECISION,
                    v8 DOUBLE PRECISION,
                    v9 DOUBLE PRECISION,
                    v10 DOUBLE PRECISION,
                    v11 DOUBLE PRECISION,
                    v12 DOUBLE PRECISION,
                    v13 DOUBLE PRECISION,
                    v14 DOUBLE PRECISION,
                    v15 DOUBLE PRECISION,
                    v16 DOUBLE PRECISION,
                    v17 DOUBLE PRECISION,
                    v18 DOUBLE PRECISION,
                    v19 DOUBLE PRECISION,
                    v20 DOUBLE PRECISION,
                    v21 DOUBLE PRECISION,
                    v22 DOUBLE PRECISION,
                    v23 DOUBLE PRECISION,
                    v24 DOUBLE PRECISION,
                    v25 DOUBLE PRECISION,
                    v26 DOUBLE PRECISION,
                    v27 DOUBLE PRECISION,
                    v28 DOUBLE PRECISION,
                    fraud SMALLINT NOT NULL

                );
            """)
            conn.commit()
            print("✅ Table 'transactions_test_raw' CREATED/EXISTS).")
    
    except Exception as e:
        print("❌ ERROR Creating table transactions_test_raw : ", e)
        if conn:
            conn.rollback()

 # loading the dataset
 # transaction train data 
def load_train_data(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO transaction_train (time_seconds, timestamp, ammount, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19,v20, v21, v22, v23, v24, v25, v26, v27, v28, fraud )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, [
                (pl['Time_elapsed_sec'], pl['Timestamp'], pl['Ammount'], pl['V1'], pl['V2'], pl['V3'], pl['V4'], pl['V5'], pl['V6'], pl['V7'], pl['V8'], pl['V9'], pl['V10'], pl['V11'], pl['V12'], pl['V13'], pl['V14'], pl['V15'], pl['V16'], pl['V17'], pl['V18'], pl['V19'], pl['V20'], pl['V21'], pl['V22'], pl['V23'], pl['V24'], pl['V25'], pl['V26'], pl['V27'], pl['V28'], pl['Fraud'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading  transaction_train_DATA",e)

 # transaction test data
def load_test_data(conn, data):
    try:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO transaction_train (time_seconds, timestamp, ammount, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19,v20, v21, v22, v23, v24, v25, v26, v27, v28, fraud )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, [
                (pl['Time_elapsed_sec'], pl['Timestamp'], pl['Ammount'], pl['V1'], pl['V2'], pl['V3'], pl['V4'], pl['V5'], pl['V6'], pl['V7'], pl['V8'], pl['V9'], pl['V10'], pl['V11'], pl['V12'], pl['V13'], pl['V14'], pl['V15'], pl['V16'], pl['V17'], pl['V18'], pl['V19'], pl['V20'], pl['V21'], pl['V22'], pl['V23'], pl['V24'], pl['V25'], pl['V26'], pl['V27'], pl['V28'], pl['Fraud'])
                for pl in data
            ])
            conn.commit()
            print("✅ Data Succesfully Loaded")
    except Exception as e:
        print("❌ ERROR in Loading  transaction_test_DATA",e)