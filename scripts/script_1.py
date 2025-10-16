import os
from dotenv import load_dotenv
from pipelines import load_to_postgress
from functions import load_csv, feat_eng, split_1


load_dotenv()

table_name1 = 'Transactions'
table_name2 = "streaming_data"
csv_path = os.getenv("csv_path")

 # loading the data from csv
data = load_csv(csv_path)

 # performing feature Engineering
feat_eng_data = feat_eng(data)
data1, data2 = split_1(feat_eng_data)

 # creating tables and loading data to the tables 
load_to_postgress(data1, table_name1)
load_to_postgress(data2,table_name2) 


