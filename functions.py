 # Libraries
import pandas as pd
from sklearn.model_selection import train_test_split
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv

 # Creating Timestamp Column
def feat_eng(df):
    start_time = pd.to_datetime("2025-09-20 00:00:00")
    df["timestamp"] = start_time + pd.to_timedelta(df["Time"], unit="s")
    df["timestamp"] = df["timestamp"].apply(lambda x: x.to_pydatetime())
    df["Time"] = df['Time'].astype(int)

    # Rename for consistency
    df = df.rename(columns={
       'Time': 'time_seconds',
        'Class': 'Fraud',
        'Amount': 'Ammount'
    })
    return df



 # splitting the dataset into train and test 
def split(df):
    tr_df, te_df = train_test_split(
        df,
        test_size=0.2,           
        random_state=42,
        stratify=df["Fraud"]
    )
    return tr_df , te_df


 # Creating tables in postgress
 # transactions_train_raw
def create_train_table( conn ):   
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions_train_raw (
                    transaction_id SERIAL PRIMARY KEY,     
                    time_seconds INT NOT NULL,                      
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
                    ammount NUMERIC(10,2) NOT NULL,
                    fraud SMALLINT NOT NULL,
                    timestamp TIMESTAMP NOT NULL
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
                    ammount NUMERIC(10,2) NOT NULL,
                    fraud SMALLINT NOT NULL,
                    timestamp TIMESTAMP NOT NULL       
                );
            """)
            conn.commit()
            print("✅ Table 'transactions_test_raw' CREATED/EXISTS).")
    
    except Exception as e:
        print("❌ ERROR Creating table transactions_test_raw : ", e)
        if conn:
            conn.rollback()

 # loading the dataset
 # transaction test data 
def load_test_data(conn, df):
    try:
        with conn.cursor() as cur:

            records = list(df.itertuples(index=False, name=None))
            columns = ', '.join(df.columns)
            
            sql = f"""
                INSERT INTO transactions_test_raw ({columns})
                VALUES %s
            """

            # Bulk insert
            execute_values(cur, sql, records, page_size=10000)

            conn.commit()
            print(f"✅ Inserted {len(records)} rows into transactions_test_raw")
    except Exception as e:
        print("❌ ERROR in Loading transactions_test_DATA", e)

 # transaction train data
def load_train_data(conn, df):
    try:
        with conn.cursor() as cur:

            records = list(df.itertuples(index=False, name=None))
            columns = ', '.join(df.columns)

            sql = f"""
                INSERT INTO transactions_train_raw ({columns})
                VALUES %s
            """

            # Bulk insert
            execute_values(cur, sql, records, page_size=10000)

            conn.commit()
            print(f"✅ Inserted {len(records)} rows into transactions_train_raw")
    except Exception as e:
        print("❌ ERROR in Loading transactions_train_DATA", e)

 # Importing data from postgres
def import_data(table_name, engine):
    try:
        data = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        print(" DATA SUCCESFULLY LOADED ")
        return data
    except Exception as e:
            print(" ERROR : COULD NOT LOAD DATA FROM DATABASE : ", e)