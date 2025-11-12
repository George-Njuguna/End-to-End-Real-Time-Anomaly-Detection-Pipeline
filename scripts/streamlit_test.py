from pipelines import fetch_batch_data
import psycopg2
import pandas as pd
import os 

import streamlit as st


# Connecting to database 
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password="aninterludecalled",
    host = "localhost",
    port="5431"
)

df = fetch_batch_data('streaming_data' , 3000 , conn )

df = pd.DataFrame(df)
x = df['fraud']==1

fraud = df[x]

st.write("### Data Table")
st.dataframe(fraud)  # interactive scrollable table

