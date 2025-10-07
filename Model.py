import pandas as pd 
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from functions import import_data
from pipelines import modeling_pipe

load_dotenv()

 # Connecting to the database 
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('database')}"
    )

 # importing the data for training
train_data = import_data('transactions_train_raw', engine )

 # Training the model
precision, recall, f1_score, parameters = modeling_pipe(train_data, False)

precision1, recall2, f1_score3, parameters4 = modeling_pipe(train_data, True)


def main():
    print("END OF THE MODELLING PIPELINE")
    
if __name__ == '__main__':
    main()