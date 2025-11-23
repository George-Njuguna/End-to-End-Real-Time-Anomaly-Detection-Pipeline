''' Build an app that:
1.  Lets user upload a CSV.
2.  Displays the head() of the DataFrame.
3.  Has a selectbox for choosing one column.
4.  Displays summary statistics of that column (df[column].describe()).
'''

import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import psycopg2
import os
from dotenv import load_dotenv
from functions import import_data
import datetime

load_dotenv()

 # table
table = "infered_transactions"



st.set_page_config(page_title="My Dashboard", layout="wide")
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

plt.rcParams["figure.facecolor"] = "none"
plt.rcParams["axes.facecolor"] = "none"

plt.rcParams["text.color"] = "white"
plt.rcParams["axes.labelcolor"] = "white"
plt.rcParams["xtick.color"] = "white"
plt.rcParams["ytick.color"] = "white"
plt.rcParams["legend.labelcolor"] = "white"
plt.rcParams["axes.titlecolor"] = "white"


# Cache the connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password="aninterludecalled",
        host="localhost",
        port="5431"
    )

# Cache the data load
@st.cache_data(ttl=60)
def load_cached_data(table):
    conn = get_connection()   # get cached connection
    return import_data(table, conn)

# load data
df = load_cached_data(table)

def get_transac_n_perc_increase(data, prev_day_filter, all_filter, curr_filter = None ):
     # getting data counts that was before curr date 
    prev_day_data = data[prev_day_filter]
    previous_day_counts = (prev_day_data.shape)[0]

     # getting data counts on the current date range
    if curr_filter is None:
        curr_day_data = data[all_filter]   
        curr_day_counts = (curr_day_data.shape)[0]

    else:
        curr_day_data = data[all_filter&curr_filter]   
        curr_day_counts = (curr_day_data.shape)[0]

     # getting the percentages increase
    if previous_day_counts == 0:
        perc = np.nan
    else:
        perc = ((curr_day_counts-previous_day_counts)/previous_day_counts)*100
    
    return curr_day_counts, perc

 
    

 # Setting sidebar 
st.sidebar.title("Filters and Settings")


st.sidebar.markdown("---")
filter_trans = st.sidebar.selectbox("Transction Type",["all","fraud","valid"])
start_dt = st.sidebar.date_input("From", value = datetime.date( 2025, 12, 1 ) )
end_dt = st.sidebar.date_input("To", value = datetime.date( 2025, 12, 31 ))
st.sidebar.markdown("---")
limit_rows = st.sidebar.slider("Max rows shown in table", min_value=50, max_value=1000, value=100, step=50)

start_date = pd.to_datetime(start_dt)
end_date = pd.to_datetime(end_dt)


all_filter = df['processed_at'].between(start_date, end_date)
previous_day_filter = df['processed_at']< start_date
valid_filter = ((df['fraud'] == 0) & (df['prediction'] == 0))
fraud_filter = ((df['fraud'] == 1) & (df['prediction'] == 1))
false_alarm_filter = ((df['fraud'] == 0) & (df['prediction'] == 1))
missed_alarm_filter = ((df['fraud'] == 1) & (df['prediction'] == 0))

all = get_transac_n_perc_increase(df , previous_day_filter , all_filter)
valid = get_transac_n_perc_increase(df , previous_day_filter, all_filter, valid_filter)
fraud = get_transac_n_perc_increase(df , previous_day_filter , all_filter, fraud_filter)
false_alarm = get_transac_n_perc_increase(df , previous_day_filter , all_filter, false_alarm_filter)
missed_alarm = get_transac_n_perc_increase(df , previous_day_filter , all_filter, missed_alarm_filter)


all_transactions = all[0]
valid_transactions = valid[0]
fraud_transactions = fraud[0]
false_alarm_transactions = false_alarm[0]
missed_alarm_transactions = missed_alarm[0]

 # percentages from previous day only shows up if end date has been choosen or when the nothing has been choosen 
all_percentages = all[1]
valid_percentages = valid[1]
fraud_percentages = fraud[1]
false_alarm_percentages = false_alarm[1]
missed_alarm_percentages = missed_alarm[1]




 # Setting the header and KPI
st.title("Near Time Transaction Monitoring Dashboard")

 # setting tabs
T1,T2 = st.tabs(["Overview", "Details"])

with T1:
    k1, k2, k3, k4, k5 = st.columns([2,2,2,2,2])
    k1.metric('All Transactions', f"{all_transactions}",f"{all_percentages}", border=True)
    k2.metric('Valid Transactions', f"{valid_transactions}" ,f"{valid_percentages}", border=True)
    k3.metric('Fradulent Transactions', f"{fraud_transactions}",f"{fraud_percentages}", border=True)
    k4.metric('False Alarm', f"{false_alarm_transactions}" ,f"{false_alarm_percentages}", border=True)
    k5.metric('Missed Fraud', f"{missed_alarm_transactions}",f"{missed_alarm_percentages}", border=True )
    st.markdown("---")
