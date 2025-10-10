import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from functions import import_data
from pipelines import modeling_pipe
from log_mlflow import mlflow_pipe, get_best_run_from_domain, get_prod_model, update_production_model
from mlflow.tracking import MlflowClient

load_dotenv()

 # Connecting to the database 
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('database')}"
    )

 # importing the data for training
train_data = import_data('transactions_train_raw', engine )

 # Training the model
model1 = modeling_pipe(train_data, False)
model2 = modeling_pipe(train_data, True)

 # logging models, metrics and artifacts to mlflow
tracking_uri = "http://127.0.0.1:5000" 
#tracking_uri =  os.getenv('tracking_uri') )
client = MlflowClient()
experiment_name = "Fraud_Detection_test"
model_name = "fraud_detection_test"
artifact_path = "fraud_model_test"
metric = "F1 Score"
domain = 'fraud'

mlflow_pipe(model1, tracking_uri, experiment_name, False, model_name, artifact_path,domain)
mlflow_pipe(model2, tracking_uri, experiment_name, True, model_name, artifact_path,domain)

 # getting best run in the fraud domain
best_run_id = get_best_run_from_domain(domain , client, metric )

 # getting current production model
prod_model_id = get_prod_model(model_name, client)

 # updating production model
update_production_model(client, model_name, best_run_id, artifact_path, prod_model_id )



def main():
    print("END OF THE MODELLING AND LOADING TO MLFLOW")
    
if __name__ == '__main__':
    main()