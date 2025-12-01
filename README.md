## Fraud Detection Pipeline — End-to-End Machine Learning System

This repository contains a fully containerized, production-style Fraud Detection Pipeline built with modern data engineering and machine learning tools. The system ingests daily transaction data, processes it through Kafka, performs fraud inference using an MLflow-stored model, retrains the model weekly through Airflow, and serves interactive analytics via a Streamlit dashboard. All components run inside Docker containers connected through a shared Docker network.

#### 📌 Project Overview

This project demonstrates a complete real-world fraud detection ecosystem with the following capabilities:

- Daily ingestion of new transactions into PostgreSQL

- Kafka streaming of new batches for real-time fraud inference

- Weekly model retraining using Airflow

- MLflow experiment tracking and model versioning

- Automated inference pipeline using Kafka Consumer + MLflow Model Registry

- Dashboarding with Streamlit for real-time visualization

- Batch & transaction ID management to prevent duplicate loads

**The model used is Logistic Regression, selected for its efficiency, interpretability, and reliability under limited computational resources.**

#### 🚀 Technology Stack

- Docker & Docker Compose	Containerization and multi-service orchestration
- PostgreSQL	Storage of raw data, processed data, and prediction results
- Kafka (Broker + Zookeeper)	Real-time streaming of transactions
- Kafka Producer	Sends newly ingested transactions to the consumer
- Kafka Consumer	Loads MLflow model and performs fraud inference
- MLflow Tracking + Model Registry	Logs experiments and stores/serves trained models
- Airflow	Orchestrates daily ingestion, weekly retraining, producer/consumer execution
- Streamlit	Interactive dashboard for visualizing fraud cases, batches, and trends
- Python	Data processing, modeling, streaming, and orchestration logic

              

#### 📊 **Data Engineering Workflow**

**1. Daily Transaction Loading (Airflow DAG)**

Each day, Airflow triggers a DAG that:

- Loads new daily transactions.

- Assigns each record a unique transaction_id.

- Groups them under a unique batch_id.

- Inserts the batch into PostgreSQL.

- Updates a batch-tracking table so:

        No batch loads more than once

        Duplicates are prevented using transaction_id checks

**2. Batch Deduplication Strategy**

To keep the database clean and ensure correct model training:

- batch_id ensures batches are uniquely tracked.

- transaction_id prevents repeated transactions.

- Processed batches are flagged and will never be re-ingested.

##### ⚡ **Streaming & Inference Pipeline**
**Kafka Producer**

- Reads the latest unprocessed batch from PostgreSQL.

- Streams transactions to Kafka.

**Kafka Consumer**

- Loads the latest Logistic Regression model from MLflow.

- Performs fraud inference on incoming transactions.

- Writes predictions back to PostgreSQL, linked to:

- transaction_id

- batch_id

This forms a complete lineage trail for auditing.

##### 📈 **Weekly Model Retraining (Airflow)**

- Once per week, Airflow triggers the model retraining pipeline:

- Loads all historical transactions (including new daily batches).

- Performs preprocessing:

- Create timestamp from base date 2025-09-20

- Scale amount

- Uses stratified sampling to handle class imbalance.

- Trains a new Logistic Regression model.

- Logs metrics, parameters, and artifacts to MLflow.

- Registers the new model version in the MLflow Model Registry.

The next streaming cycle will automatically use the latest approved model.

##### 🖥️ Streamlit Dashboard

The dashboard provides:

- Real-time fraud predictions 

- Weekly model performance trends

- Historical fraud rate visualizations

- Insights from EDA and modeling

This offers complete transparency into the fraud detection lifecycle.

#### 🔗  **System Architecture**

                                  ┌────────────────────────────┐
                                  │           AIRFLOW          │
                                  │  - Daily ingestion DAGs    │
                                  │  - Weekly retraining       │
                                  │  - Producer/consumer runs  │
                                  │  - Batch tracking          │
                                  └────────────────────────────┘
                                           │
                                           ▼
                 ┌─────────────────────────────────────────────────────┐
                 │                     POSTGRESQL                      │
                 │   - Stores raw and processed transactions           │
                 │   - Tracks batch_id & transaction_id                │
                 │   - Stores inference outputs                        │
                 └─────────────────────────────────────────────────────┘
                                           │
                                           ▼
                        ┌────────────────────────────┐
                        │           KAFKA            │
                        │ Producer → streams batches │
                        │ Consumer → runs inference  │
                        └────────────────────────────┘
                                           │
                                           ▼
                       ┌──────────────────────────────┐
                       │        KAFKA CONSUMER         │
                       │ - Loads MLflow model          │
                       │ - Writes predictions to DB    │
                       └──────────────────────────────┘
                                           │
                                           ▼
                      ┌────────────────────────────────┐
                      │             MLflow              │
                      │ - Logs experiments              │
                      │ - Stores model versions         │
                      │ - Serves model for inference    │
                      └────────────────────────────────┘
                                           │
                                           ▼
                 ┌────────────────────────────────────────────┐
                 │                  STREAMLIT                 │
                 │ - Fraud dashboard (real-time + historical) │
                 └────────────────────────────────────────────┘



##### 📄 Requirements

Below is the content for requirements.txt:

- pandas
- numpy
- scikit-learn
- psycopg2-binary
- streamlit
- mlflow
- kafka-python
- apache-airflow
- sqlalchemy
- python-dotenv
- plotly


Feel free to add/remove dependencies based on your implementation.

📚 Key Features Summary

- Automated daily ingestion of new transactions

- Weekly logistic regression model retraining

- MLflow model tracking and versioning

- Robust Kafka streaming pipeline

- Real-time fraud prediction

- Full end-to-end batch lineage and deduplication

- Professional Streamlit dashboard

- Completely containerized architecture

- Airflow orchestrated workflows

##### 🌱 Future Work

- Integrate more advanced models (Random Forest, XGBoost)

- Real-time alerting system for high-risk fraud

- API gateway for model serving

- Docker Swarm or Kubernetes deployment

- Grafana dashboards for infrastructure monitoring
