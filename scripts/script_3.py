from log_mlflow import mlflow_pipe, get_best_run_from_domain, get_prod_model, update_production_model

tracking_uri = "http://mlflow:5001"
experiment_name = "Fraud_Detection_test"
model_name = "fraud_detection_test"
artifact_path = "fraud_model_test"
metric = "F1_Score"
domain = 'fraud'

 # getting best run in the fraud domain
best_run_id = get_best_run_from_domain( domain , metric, tracking_uri)

 # getting current production model
prod_model_id = get_prod_model(model_name, tracking_uri)

 # updating production model
update_production_model( model_name, best_run_id, artifact_path, prod_model_id,tracking_uri )



def main():
    print("END OF ADDING MODEL TO PRODUCTION ")
    
if __name__ == '__main__':
    main()