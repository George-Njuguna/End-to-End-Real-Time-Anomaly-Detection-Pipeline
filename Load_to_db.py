 # Importing libraries
from dotenv import load_dotenv
from functions import feat_eng, split,load_csv
from pipelines import load_to_postgress

load_dotenv()

 # Loading the CSV FILE 
DATA = load_csv('creditcard.csv')

 # feature engineering
try:
    data = feat_eng(DATA)
except Exception as e:
    print(" ERROR IN [ADDING TIMESTAMP AND CHANGING COLUMN NAMES]:", e)

 # splitting the data
try:
    train_df, test_df = split(data)
except Exception as e:
    print(" ERROR IN [SPLITTING THE DATASETS]:", e)

 # LOADING THE DATA TO POSTGRESS
load_to_postgress(train_df, test_df)

def main():
    print("END OF THE LOADING TO DB")
    
if __name__ == '__main__':
    main()