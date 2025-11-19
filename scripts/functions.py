 # Libraries
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from psycopg2.extras import execute_values


 # Loading CSV file
def load_csv(file_path):
    if not isinstance(file_path, str):
        raise ValueError("Input 'file_path' should be a file path string")
    """
    Reads a csv file in the same directory.

    Parameters
    ----------
    : str.

    Returns
    -------
    df : Dataframe
    """

    try:
        data = pd.read_csv(file_path)
        print(' CSV SUCESSFULLY LOADED ')
        return data
    except Exception as e:
        print(" ERROR IN READING CSV FILE:", e)




 # Creating Timestamp Column
def feat_eng(df):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input 'df' must be a pandas DataFrame!")
    """ 
        Changes Time column to an int.
        Renames columns Time , Class and Amount 

    Parameters
    ----------
    df : pandas.DataFrame.

    Returns
    -------
    df : Dataframe
    """
    
    df["Time"] = df['Time'].astype(int)

    # Rename for consistency
    df = df.rename(columns={
       'Time': 'time_seconds',
        'Class': 'Fraud',
        'Amount': 'Ammount'
    })
    return df




 # splitting the dataset into train and test 
def split_1(df):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input 'df' must be a pandas DataFrame!")
    """ 
    Splits the data into train and Test sets

    Parameters
    ----------
    df : pandas.DataFrame.

    Returns
    -------
    tuple
        (tr_df, te_df)  
    """
    
    tr_df, te_df = train_test_split(
        df,
        test_size=0.2,           
        random_state=42,
        stratify=df["Fraud"]
    )
    return tr_df , te_df


 # splitting data to independent and dependent variables
def split_func(df):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input 'df' must be a pandas DataFrame!")
    """
     Splits the data into Dependant and independent Variables 

    Parameters
    ----------
    df : pandas.DataFrame.

    Returns
    -------
    df : Dataframe
    """ 

    try:
        X = df.iloc[ :,2:-1 ]
        y = df.iloc[ :,-1 ]
        return X, y
    except Exception as e:
        print(" ERROR : COULD NOT SPLIT TO VARIABLES : ", e)


 # aligning columns arrangements
def align_df_to_table(conn, df, table_name):
    """
    Aligns a DataFrame's columns to match the exact column order in a PostgreSQL table
    (excluding SERIAL/AUTO columns such as transaction_id).
    """
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        db_columns = [row[0] for row in cur.fetchall()]
    
    db_columns = [col for col in db_columns if col != "transaction_id"]
    print('Colums order', db_columns)

    df_columns = df.columns.tolist()
    
     # Cheking missing columns
    missing = set(db_columns) - set(df_columns)
    if missing:
        raise ValueError(f"DataFrame is missing required columns: {missing}")
    
     # Checking extra columns
    extra = set(df_columns) - set(db_columns)
    if extra:
        print(f"⚠️ DataFrame has extra columns not in table and will be ignored: {extra}")

    # Reindex DF to correct order (dropping extra columns)
    df = df[db_columns]
    print("ordered database colums", df.columns.to_list)

    print("✅ DataFrame successfully aligned to table column order")
    return df


 # Creating table in postgress
def create_table( conn, table_name ):  
    """    
    Creates table with name 'table_name' if it doesnt exist

    Parameters
    ----------
    conn : connection to the dataframe 
    table_name : name of the table being created
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
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
                    fraud SMALLINT NOT NULL
                );
            """)
            conn.commit()
            print(f"✅ Table '{table_name}' CREATED/EXISTS).")
    
    except Exception as e:
        print(f"❌ ERROR Creating {table_name} : ", e)
        if conn:
            conn.rollback()

 # Creating the predictions table
def create_prediction_table( conn, table_name ):  
    """    
    Creates table with name 'table_name' if it doesnt exist

    Parameters
    ----------
    conn : connection to the dataframe 
    table_name : name of the table being created
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    transaction_id SERIAL PRIMARY KEY,
                    processed_at TIMESTAMPTZ,   
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
                    prediction SMALLINT NOT NULL,
                    probability DOUBLE PRECISION
                );
            """)
            conn.commit()
            print(f"✅ Table '{table_name}' CREATED/EXISTS).")
    
    except Exception as e:
        print(f"❌ ERROR Creating {table_name} : ", e)
        if conn:
            conn.rollback()



 # loading the dataset
 # transaction test data 
def load_data(conn, df, table_name):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input 'df' must be a pandas DataFrame!")
    """    
    Loads the Data in the specified 'table_name'

    Parameters
    ----------
    conn : connection to the Database
    df : pd.Dataframe containing the data to be loaded
    table_name : str representing the table name where the data is being loaded
    """
    try:
        with conn.cursor() as cur:
            df = align_df_to_table(conn, df, table_name) 
            
            records = list(df.itertuples(index=False, name=None))
            columns = ', '.join(df.columns)
            
            sql = f"""
                INSERT INTO {table_name} ({columns})
                VALUES %s
            """

            # Bulk insert
            execute_values(cur, sql, records, page_size=10000)

            conn.commit()
            print(f"✅ Inserted {len(records)} rows into {table_name}")
    except Exception as e:
        print("❌ ERROR in Loading data in {table_name}", e)



 # Importing data from postgres
def import_data(table_name, conn):
    if not isinstance(table_name, str):
        raise ValueError("Input 'table_name' should be a string repin the table name")
    """    
    Imports the data from postgress
    
    Parameters
    ----------
    table_name : name of the table in the database
    conn : connection to the dataframe 

    Returns
    -------
    df : pd.Dataframe
    """
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        print("✅  DATA SUCCESFULLY LOADED ")
        return df
    except Exception as e:
            print("❌ ERROR : COULD NOT LOAD DATA FROM DATABASE : ", e)




