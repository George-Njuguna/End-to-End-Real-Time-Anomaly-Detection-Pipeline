import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns


timestamp = datetime.now().strftime( "%Y-%m-%d" )

def mlflow_pipe(model_info, tracking_uri,experiment, imbalance_handling):
    if not isinstance(model_info, dict):
        raise ValueError("Input 'model_info' must be a dictonary")

    if not isinstance(imbalance_handling, bool):
        raise ValueError("Input 'imbalance_handling' must be either True or False (boolean type)!")
    
    if not isinstance(tracking_uri, str):
        raise ValueError("Input 'tracking_uri' must be a string value representing the uri")
    
    if not isinstance(experiment, str):
        raise ValueError("Input 'experiment' must be a string representing the experiment name")
    
    try:
        mlflow.set_tracking_uri( uri = tracking_uri )
        mlflow.set_experiment(experiment)

        with mlflow.start_run( run_name = f"Logistic_model{timestamp}" ) as run:
            mlflow.sklearn.log_model(
                sk_model = model_info[4],
                artifact_path = "fraud_model",
                registered_model_name= "fraud_detection"
            )

            mlflow.log_metric('Precision', model_info[0])
            mlflow.log_metric('Recall', model_info[1])
            mlflow.log_metric('F1 Score', model_info[2])

            mlflow.log_params(model_info[5])

            mlflow.set_tag("dataset_version", "v1")
            if imbalance_handling == False:
                mlflow.set_tag("Imbalance Handling", "None")
            else:
                mlflow.set_tag("Imbalance Handling", "SMOTE")

            mlflow.set_tag("Trained_at", f"{timestamp}")
            mlflow.set_tag("domain", "fraud")

            
            report = model_info[3]

            with open("classification_report.txt", "w") as f:
                f.write(report)

            mlflow.log_artifact("classification_report.txt")

            cm = model_info[6]
            
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




