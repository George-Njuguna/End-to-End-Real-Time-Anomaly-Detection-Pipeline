import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns


timestamp = datetime.now().strftime( "%Y_%m_%d" )

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

        with mlflow.start_run( run_name = f"Logistic_model_{timestamp}" ) as run:
            mlflow.sklearn.log_model(
                sk_model = model_info["model"],
                name = artifact_path,
                registered_model_name= model_name,
                signature= model_info['signature']
            )

            mlflow.log_metric('Precision', model_info["precision"])
            mlflow.log_metric('Recall', model_info["recall"])
            mlflow.log_metric('F1_Score', model_info["f1_score"])

            mlflow.log_params(model_info["parameters"])
            mlflow.log_param("Data_version", model_info['data_version'])
            mlflow.log_param("Rows", model_info['rows_number'])
            mlflow.log_param("Columns", model_info['columns'])

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
def get_best_run_from_domain(domain, client, metric, tracking_uri):
    if not isinstance( domain, str ):
        raise ValueError("Input 'domain' must be a string repin domain tag")
    if not isinstance( metric, str ):
        raise ValueError("Input 'metric' must be a string repin the performance metric")
    """ 
    gets best run id based on the highest 'metric'  

    Parameters
    ----------
    domain : str that representing the domain tag in a run in mlflow
    client : Mlflow tracking client 
    metric : metric ie F1_score , Recall etc
    tracking_uri : str , tracking uri of mlflow

    Returns
    -------
    best_run_id
    """
    mlflow.set_tracking_uri( uri = tracking_uri )

    try:

        experiments = client.search_experiments()
        best_run = None
        best_score = float('-inf')

        for exp in experiments:
            runs = client.search_runs(
                experiment_ids=[exp.experiment_id],
                filter_string=f"tags.domain = '{domain}'",
                order_by=[f"metrics.{metric} DESC"],
                max_results=1
            )

            if runs and len(runs) > 0:
                run = runs[0]
                score = run.data.metrics.get(metric, None)

                if score is not None and score > best_score:
                    best_score = score
                    best_run = run

        if best_run is None:
            print(f"No runs with metric '{metric}' found in domain '{domain}'.")
            return None

        best_run_id = best_run.info.run_id
        print(f"BEST RUN = {best_run_id}, BEST {metric} SCORE = {best_score:.4f}")
        return best_run_id

    except Exception as e:
        print(f"ERROR IN get_best_run_from_domain: {e}")
        return None



# Getting production model for a domain
def get_prod_model(model_name, client , tracking_uri):
    """ 
    gets the current production model with a certain model name ie if model names are fraud_detection_model gets production model with that name  

    Parameters
    ----------
    model_name : str that representing the model name given while running and logging the experiment 
    client : Mlflow tracking client
    tracking_uri : str , tracking uri of mlflow

    Returns
    -------
    production model run ID if there is a production model if not returns none 

    """
    mlflow.set_tracking_uri( uri = tracking_uri )
    try:
        model_version = client.get_model_version_by_alias(model_name, "Production")
        return model_version.run_id
    
    except Exception as e:
        if "Registered model alias Production not found" in str(e):
            return None  
        else:
            print("ERROR IN get_prod_model:", e)




# checking if best model is new or same as production model and promoting it to production 
def update_production_model(client, model_name, best_run_id, artifact_path, curr_prod_id, tracking_uri):
    """ 
    checks the best run id and compares it to the current production id if they dont match the best_run_id model gets updated to production.

    Parameters
    ----------
    model_name : str that representing the model name given while running and logging the experiment
    client : Mlflow tracking client 
    best_run_id : run id returned from get_best_run_from_domain function
    artifact_path : artifact path set while logging model to mlflow
    curr_prod_id : the production run id that is returned from get_prod_model
    tracking_uri : str , tracking uri of mlflow

    """
    mlflow.set_tracking_uri( uri = tracking_uri )

    try:
        if best_run_id == curr_prod_id:
            print("NO UPDATE NEEDED")
            

        else:
            versions = client.search_model_versions(f"name='{model_name}'")

            target_version = None

            for v in versions:
                if v.run_id == best_run_id:
                    target_version = v.version
                    break

            if target_version == None:    
                model_uri = f"runs:/{best_run_id}/{artifact_path}"
                registered_model = mlflow.register_model(model_uri, model_name)
                target_version = registered_model.version
                print(f" Registered new version {target_version} for run {best_run_id}")

            else:
                print(f"Existing version {target_version} found for best run {best_run_id}")                

            #Transitioning it to production
            client.set_registered_model_alias(
                name = model_name,
                version = target_version,
                alias = "Production"
            )

            print(f"New Model Version{target_version} promoted to Production")

    except Exception as e:
        print("ERROR in update_production_model ", e)

            

        










