#!/bin/bash

source .env 

BACKEND_URI="postgresql+psycopg2://${mlflow_user}@${host}:{port}/${mlflow_database}"
MLFLOW_ARTIFACTS_DIR = "file:${MLFLOW_ARTIFACTS_PATH}" 

echo "Starting MLflow Server..."

exec mlflow server \
    --backend-store-uri "${BACKEND_URI}" \
    --artifacts-destination "file:${MLFLOW_ARTIFACTS_DIR}" \
    --host 0.0.0.0 \
    --port 5000