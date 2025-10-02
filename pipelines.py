from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline as pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import ParameterGrid
from sklearn.model_selection import train_test_split, StratifiedKFold, GridSearchCV
from sklearn.metrics import  precision_score , recall_score , f1_score, classification_report
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import os
from functions import split_func, create_train_table, create_test_table, load_test_data,load_train_data

load_dotenv()

# LOADING DATA TO POSTGRESS PIPELINE 
def load_to_postgress(train_df, test_df):
    assert isinstance(train_df, pd.DataFrame), 'Dataframe Only!'
    assert isinstance(test_df, pd.DataFrame), 'Dataframe Only!'

    """
    Creates Train and Test Tables if they dont exist
    Loads  Data in the Created tables    

    Parameters
    ----------
    train_df : pd.DataFrame 
    test_df : pd.DataFrame
    """

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
        print("❌ ERROR IN LOADING PIPELINE:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("🔌 CONNECTION CLOSED")



# MODELLING PIPELINE
def modeling_pipe(data):

    """    
    Models the data using Logistic Regression 
    Optimizes using grid search 
    Gets the best parameters
    prints Classification Report

    Parameters
    ----------
    data = pd.DataFrame

    Returns
    -------
    precision, Recall, f1, Best_Parameters
    """

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
        pipe = pipeline([
            ('processor', processor),
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
            verbose = 1

        )

        print("Running GridSearchCV...")
        grid.fit(X_train, y_train)


        # Evaluation
        best_model1= grid.best_estimator_

        y_pred1 = best_model1.predict(X_test)

        precision = precision_score(y_test, y_pred1)
        recall = recall_score(y_test, y_pred1)
        f1 = f1_score(y_test, y_pred1)
        report = classification_report(y_test, y_pred1, output_dict=True)
        report_df = pd.DataFrame(report).transpose()
        Best_Parameters = grid.best_params_
        
        print('****CLASSIFICATION REPORT*****')
        print(report_df)
        print('Best Parameters : ', Best_Parameters)

        return precision, recall, f1, Best_Parameters
    
    except Exception as e:
        print(" ERROR : IN THE MODELLING PIPELINE : ", e)

