import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns


timestamp = datetime.now().strftime( "%Y-%m-%d" )

def mlflow_pipe(model_info, tracking_uri,experiment_name, imbalance_handling):

    if not isinstance(model_info, dict):
        raise ValueError("Input 'model_info' must be a dictonary what is returned after 'modelling_pipe'")

    if not isinstance(imbalance_handling, bool):
        raise ValueError("Input 'imbalance_handling' must be either True or False (boolean type)!")
    
    if not isinstance(tracking_uri, str):
        raise ValueError("Input 'tracking_uri' must be a string value representing the uri")
    
    if not isinstance(experiment_name, str):
        raise ValueError("Input 'experiment_name' must be a string representing the experiment name")
    
    try:
        mlflow.set_tracking_uri( uri = tracking_uri )
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run( run_name = f"Logistic_model{timestamp}" ) as run:
            mlflow.sklearn.log_model(
                sk_model = model_info["model"],
                artifact_path = "fraud_model_test",
                registered_model_name= "fraud_detection_test"
            )

            mlflow.log_metric('Precision', model_info["precision"])
            mlflow.log_metric('Recall', model_info["recall"])
            mlflow.log_metric('F1 Score', model_info["f1_score"])

            mlflow.log_params(model_info["parameters"])

            #mlflow.set_tag("dataset_version", "v1")

            if imbalance_handling == False:
                mlflow.set_tag("Imbalance Handling", "None")
            else:
                mlflow.set_tag("Imbalance Handling", "SMOTE")

            mlflow.set_tag("Trained_at", f"{timestamp}")
            mlflow.set_tag("domain", "fraud")

            
            report = model_info["classification_report"]

            with open("classification_report.txt", "w") as f:
                f.write(report)

            mlflow.log_artifact("classification_report.txt")

            cm = model_info["confusion_matrix"]
            
            plt.figure(figsize=(6, 5))
            sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
            plt.title("Confusion Matrix")
            plt.xlabel("Predicted Label")
            plt.ylabel("True Label")
            plt.tight_layout()
            plt.savefig("confusion_matrix.png")
            plt.close()

            mlflow.log_artifact("confusion_matrix.png")
            print("DONE LOADING TO MLFLOW")

    except Exception as e:
        print("ERROR : ", e)




