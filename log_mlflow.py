import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns

mlflow.set_tracking_uri( uri = "http:/127.0.0.1:5000" )
#mlflow.set_tracking_uri( os.getenv('tracking_uri') )
mlflow.set_experiment("Fraud_Detection_test")

timestamp = datetime.now().strftime( "%Y-%m-%d" )

def mlflow_pipe(model_info, imbalance_handling):

    try:
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




