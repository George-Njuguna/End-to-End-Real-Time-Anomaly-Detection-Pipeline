import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns


timestamp = datetime.now().strftime( "%Y-%m-%d" )

def mlflow_pipe(model_info, tracking_uri,experiment_name, imbalance_handling, model_name ,artifact_path, domain):

    if not isinstance(model_info, dict):
        raise ValueError("Input 'model_info' must be a dictonary what is returned after 'modelling_pipe'")

    if not isinstance(imbalance_handling, bool):
        raise ValueError("Input 'imbalance_handling' must be either True or False (boolean type)!")
    
    if not isinstance(tracking_uri, str):
        raise ValueError("Input 'tracking_uri' must be a string value representing the uri")
    
    if not isinstance(experiment_name, str):
        raise ValueError("Input 'experiment_name' must be a string representing the experiment name")
    
    if not isinstance(model_name, str):
        raise ValueError("Input 'model_name' must be a string representing the model name")
    
    """    
    logs model , parameters metrics and artifacts to mlflow

    Parameters
    ----------
    model_info : dict that is returned after running the modelling pipeline model_pipe
    tracking_uri : str , tracking uri of mlflow
    experiment_name : str name of the experiment being loaded to mlflow
    imbalance_handling : bool true repin model that used smote and vice versa
    
    """
    
    try:
        mlflow.set_tracking_uri( uri = tracking_uri )
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run( run_name = f"Logistic_model{timestamp}" ) as run:
            mlflow.sklearn.log_model(
                sk_model = model_info["model"],
                artifact_path = artifact_path,
                registered_model_name= model_name
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
            mlflow.set_tag("domain", domain)

            
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


# Getting all experiments from a specific domain and getting best run 
def get_best_run_from_domain(domain, client, metric):
    
    try:
        experiments = [
            exp for exp in client.search_experiments()
            if exp.tags.get("domain") == domain
        ]
        
        best_run = None
        best_score = float('-inf')

        for exp in experiments:
            runs = client.search.runs(
                experiment_ids = [exp.experiment_id],
                order_by = [f"merics.{metric} DESC"],
                max_result =1 
            )

            if runs:
                run = runs[0]
                score = run.data.metrics.get(metric, 0)

                if score > best_score :
                    best_score = score
                    best_run = run
                    

        best_run_id = best_run.info.run_id
        print(f"BEST RUN = {best_run} , BEST {metric} SCORE = {best_score} , best_run_id = {best_run_id} ")

        return  best_run_id
        
    
    except Exception as e:
        print("ERROR IN get_best_run_from_domain", e)


# Getting production model for a domain
def get_prod_model(model_name, client):

    try:
        curr_prod_aliases = client.get_latest_versions(model_name, aliases = ["Production"])

        if curr_prod_aliases:
            curr_prod_run_id = curr_prod_aliases[0].run_id

        else:
            curr_prod_run_id = None
        
        return curr_prod_run_id
    
    except Exception as e:
        print("ERROR IN get_prod_model")



# checking if best model is new or same as production model and promoting it to production 
def update_production_model(client, model_name, best_run_id, artifact_path, curr_prod_id):

    try:
        if best_run_id == curr_prod_id:
            print("NO UPDATE NEEDED")

        else:
            model_uri = f"runs:/{best_run_id}/{artifact_path}"
            registered_model = mlflow.register_model(model_uri, model_name)
            version = registered_model.version

            #Transitioning it to production
            client.transition_model_version_stage(
                name = model_name,
                version = version,
                stage = "Production",
                archive_existing_versions = True
            )

            print(f"New Model Version{version} promoted to Production")

    except Exception as e:
        print("ERROR in update_production_model ", e)

            

        

    









