import pandas as pd 
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from functions import import_data

load_dotenv()

 # Connecting to the database 
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('database')}"
    )

train_data = import_data('transactions_train_raw', engine )


def main():
    print("END OF THE MODELLING PIPELINE")
    
if __name__ == '__main__':
    main()