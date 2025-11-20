import os
from dotenv import load_dotenv
from pipelines import load_to_postgress,update_last_transaction_id_pipe, load_batches_pipe
from functions import load_csv, feat_eng, split_1
import pandas as pd


load_dotenv()

table_name1 = 'Transactions'
last_id_table = 'transaction_id_table'
batch_table = 'batch_table'
table_name2 = "streaming_data"
csv_path = os.getenv("csv_path")

 # creating dates from dec 1st - 32st
start_date = '2025-12-01'
end_date = '2025-12-31'

data3 = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq='D'),columns = ['date'] ) 
# batches
data3['batch'] = pd.Series([1874, 1185, 2090, 1187, 2460, 2906, 1460, 1729, 2222, 1928, 1597, 2404, 1503, 1083, 1177, 1796, 2905, 2980, 2309, 1374, 1079, 1989, 1442, 2816, 1690, 1893, 1957, 1009, 1080, 1699, 2139])
data3['status']= pd.Series([0 for _ in range(31)])

 # loading the data from csv
data = load_csv(csv_path)

 # performing feature Engineering
feat_eng_data = feat_eng(data)
data1, data2 = split_1(feat_eng_data)

 # creating tables and loading data to the tables 
update_last_transaction_id_pipe(last_id_table, 'first')
load_batches_pipe(data3,batch_table)
load_to_postgress(data1, table_name1)
load_to_postgress(data2,table_name2) 


