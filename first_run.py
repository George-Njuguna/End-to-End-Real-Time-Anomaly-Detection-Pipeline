import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from functions import import_data
from pipelines import modeling_pipe
from log_mlflow import mlflow_pipe

load_dotenv()

 # Connecting to the database 
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('database')}"
    )

 # importing the data for training
train_data = import_data('transactions_train_raw', engine )

 # Training the model
model1 = modeling_pipe(train_data, False)
#model2 = modeling_pipe(train_data, True)

 # logging models, metrics and artifacts to mlflow
tracking_uri = "http://127.0.0.1:5000" 
#tracking_uri =  os.getenv('tracking_uri') )
experiment_name = "Fraud_Detection_test"

mlflow_pipe(model1, tracking_uri, experiment_name, False )


def main():
    print("END OF THE MODELLING AND LOADING TO MLFLOW")
    
if __name__ == '__main__':
    main()