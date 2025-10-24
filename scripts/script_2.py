import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from functions import import_data
from pipelines import modeling_pipe
from log_mlflow import mlflow_pipe, get_best_run_from_domain, get_prod_model, update_production_model
from mlflow.tracking import MlflowClient

load_dotenv()
client = MlflowClient()

table_name = 'Transactions'
tracking_uri = "http://mlflow:5001"
experiment_name = "Fraud_Detection_test"
model_name = "fraud_detection_test"
artifact_path = "fraud_model_test"
metric = "F1_Score"
domain = 'fraud'

 # Connecting to the database 
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PW')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

 # importing the data for training
conn = engine.raw_connection()

train_data = import_data( table_name, conn )

 # Training the model
model1 = modeling_pipe(train_data, False)
model2 = modeling_pipe(train_data, True)



 # loading the models to mlflow 
mlflow_pipe(model1, tracking_uri, experiment_name, False, model_name, artifact_path,domain)
mlflow_pipe(model2, tracking_uri, experiment_name, True, model_name, artifact_path,domain)

 # getting best run in the fraud domain
best_run_id = get_best_run_from_domain( domain , client, metric )

 # getting current production model
prod_model_id = get_prod_model(model_name, client)

 # updating production model
update_production_model( client, model_name, best_run_id, artifact_path, prod_model_id )



def main():
    print("END OF THE MODELLING AND LOADING TO MLFLOW")
    
if __name__ == '__main__':
    main()