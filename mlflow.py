from first_run import model1, model2
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from sklearn.metrics import confusion_matrix, classification_report
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns
import pandas as pd
import os 

mlflow.set_tracking_uri( uri = "http:/127.0.0.1:5000" )
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

    cm = model1[6]
    
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



