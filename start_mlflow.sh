#!/bin/bash

source .env 

BACKEND_URI="postgresql+psycopg2://${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}"
MLFLOW_ARTIFACTS_DIR="file:${MLFLOW_ARTIFACTS_PATH}"

echo "Starting MLflow Server..."

exec mlflow server \
    --backend-store-uri "${BACKEND_URI}" \
    --default-artifact-root "${MLFLOW_ARTIFACTS_DIR}" \
    --host 0.0.0.0 \
    --port 5000