import pandas as pd 
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import mlflow.sklearn
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from functions import import_data
from pipelines import modeling_pipe

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

 # loading to mlflow
mlflow.set_tracking_uri( uri = "http://127.0.0.1:5000" )
#mlflow.set_tracking_uri( os.getenv('tracking_uri') )
mlflow.set_experiment("Fraud_Detection_test")

timestamp = datetime.now().strftime( "%Y-%m-%d" )

with mlflow.start_run( run_name = f"Logistic_without_smote_{timestamp}" ) as run:
    mlflow.sklearn.log_model(
        sk_model = model1[4],
        artifact_path = "fraud_model",
        registered_model_name= "fraud_detection"
    )

    mlflow.log_metric('Precision', model1[0])
    mlflow.log_metric('Recall', model1[1])
    mlflow.log_metric('F1 Score', model1[2])

    mlflow.set_tag("dataset_version", "v1")
    mlflow.set_tag("Imbalance Handling", "None")
    mlflow.set_tag("Trained_at", f"{timestamp}")

    cm = model1[5]
    
    plt.figure(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
    plt.title("Confusion Matrix")
    plt.xlabel("Predicted Label")
    plt.ylabel("True Label")
    plt.tight_layout()
    plt.savefig("confusion_matrix.png")
    plt.close()

    mlflow.log_artifact("confusion_matrix.png")


with mlflow.start_run( run_name = f"Logistic_with_smote_{timestamp}" ) as run:
    mlflow.sklearn.log_model(
        sk_model = model1[4],
        artifact_path = "fraud_model",
        registered_model_name= "fraud_detection"
    )

    mlflow.log_metric('Precision', model1[0])
    mlflow.log_metric('Recall', model1[1])
    mlflow.log_metric('F1 Score', model1[2])

    mlflow.set_tag("dataset_version", "v1")
    mlflow.set_tag("Imbalance Handling", "None")
    mlflow.set_tag("Trained_at", f"{timestamp}")

    cm = model1[5]
    
    plt.figure(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
    plt.title("Confusion Matrix")
    plt.xlabel("Predicted Label")
    plt.ylabel("True Label")
    plt.tight_layout()
    plt.savefig("confusion_matrix.png")
    plt.close()

    mlflow.log_artifact("confusion_matrix.png")


def main():
    print("END OF THE MODELLING AND LOADING TO MLFLOW")
    
if __name__ == '__main__':
    main()