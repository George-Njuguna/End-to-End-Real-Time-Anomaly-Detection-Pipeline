from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline as pipeline
from sklearn.compose import ColumnTransformer
from mlflow.models.signature import infer_signature
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as smtpipeline
from sklearn.model_selection import train_test_split, StratifiedKFold, GridSearchCV
from sklearn.metrics import  precision_score , recall_score , f1_score, classification_report, confusion_matrix
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os
from functions import split_func, create_table, load_data

load_dotenv()

# LOADING DATA TO POSTGRESS PIPELINE 
def load_to_postgress(df , table_name):

    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input 'train_df' must be a pandas DataFrame!")
    

    """
    Creates Train and Test Tables in postgres if they dont exist
    Loads  Data in the Created tables    

    Parameters
    ----------
    df : pd.DataFrame 
    table_name : name of the table being loaded
    """

    try:
        # connecting to the database
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PW'),
            host = os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        print('✅ Connection made')

        # Checking if the tables Exist/Creating The Tables
        create_table(conn, table_name)
        
        # Loading The Data
        load_data(conn, df, table_name)

    except Exception as e:
        print("❌ ERROR IN LOADING PIPELINE:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("🔌 CONNECTION CLOSED")

 # Fetching batches of data from postgress
def fetch_batch_data(table_name, batch_size, conn, last_id=0):
    """
    Imports data from PostgreSQL in batches, fetching only new rows
    based on transaction_id > last_id.
    """
    try:
        query = f"""
            SELECT * FROM {table_name}
            WHERE transaction_id > {last_id}
            ORDER BY transaction_id
            LIMIT {batch_size};
        """
        df = pd.read_sql(query, conn)
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"❌ ERROR in fetch_batch_data: {e}")
        return []






# MODELLING PIPELINE
def modeling_pipe(data, imbalance_handling):

    if not isinstance(data, pd.DataFrame):
        raise ValueError("Input 'data' must be a pandas DataFrame!")

    if not isinstance(imbalance_handling, bool):
        raise ValueError("Input 'imbalance_handling' must be either True or False (boolean type)!")

    """    
    Models the data using Logistic Regression 
    Optimizes using grid search 
    if imbalance_handling true uses SMOTE else none
    Gets the best parameters
    prints Classification Report

    Parameters
    ----------
    data = pd.DataFrame

    Returns
    -------
    a dictonary that contains precision, Recall, f1, classification_report, best_model, Best_Parameters
    """
     # getting data parameters
    data_version = f"From Transaction ID {data.iloc[0,0]} To {data.iloc[-1,0]}"
    rows_number = len(data)
    columns = list(data.iloc[:,2:-1].columns)

     # splitting variables(dependent, independent)
    X, y = split_func(data)

     # Splitting the dataset(train, test)
    X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
    )
    
    try:
        # Scalling ammount column
        scaled_col = ['ammount']

        processor = ColumnTransformer(
            transformers = [
                ('scale_column', StandardScaler(), scaled_col )
            ],
            remainder = 'passthrough'
        )

        # pipeline
        if imbalance_handling == False:
            print("Modelling without Handling Imbalance.....")
            pipe = pipeline([
                ('processor', processor),
                ('clf', LogisticRegression( max_iter=1000 ))
            ])
        else:
            print("Handling Imbalance with SMOTE......")
            pipe = smtpipeline([
            ('processor', processor),
            ('smote', SMOTE(random_state = 42)),
            ('clf', LogisticRegression( max_iter=1000 ))
        ])

        param_grid = [
            {'clf__penalty':['l2'],
            'clf__C' : [0.01,0.1,1,10,100],
            'clf__solver': ['lbfgs']
        }
        ]

        # cross validation
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

        grid = GridSearchCV(
            estimator = pipe,
            param_grid = param_grid,
            scoring = 'f1',
            cv = skf,
            n_jobs = 1,
            verbose = 2

        )

        print("Running GridSearchCV...")
        grid.fit(X_train, y_train)


        # Evaluation
        best_model= grid.best_estimator_

        y_pred = best_model.predict(X_test)

        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        class_report = classification_report(y_test, y_pred, target_names=["No Fraud", "Fraud"])
        cm = confusion_matrix(y_test, y_pred)
        Best_Parameters = grid.best_params_
        signature = infer_signature( X_train, best_model.predict(X_train) )
        print("COMPLETED MODELLING....")

        return {
            "precision":precision,
            "recall":recall,
            "f1_score":f1,
            "classification_report":class_report,
            "model":best_model,
            "parameters":Best_Parameters,
            "confusion_matrix":cm,
            "signature":signature,
            "data_version":data_version,
            "rows_number":rows_number,
            "columns":columns
        }
    
    
    
    except Exception as e:
        print(" ERROR : IN THE MODELLING PIPELINE : ", e)

