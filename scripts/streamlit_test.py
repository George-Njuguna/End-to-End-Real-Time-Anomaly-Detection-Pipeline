''' Build an app that:
1.  Lets user upload a CSV.
2.  Displays the head() of the DataFrame.
3.  Has a selectbox for choosing one column.
4.  Displays summary statistics of that column (df[column].describe()).
'''

import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import psycopg2
import os
from dotenv import load_dotenv
from functions import import_data
from datetime import datetime, timedelta

 # table
table = "infered_statistics"


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


 # loading from postgress(test)
@st.cache_data(ttl=60)

def load_cached_data(table, conn_params):
    return import_data(table, conn_params)

conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password="aninterludecalled",
    host = "localhost",
    port="5431"
)

df = load_cached_data( table, conn )

 # Setting sidebar 
st.sidebar.title("Filters and Settings")


st.sidebar.markdown("---")
filter_trans = st.sidebar.selectbox("Transction Type",["all","fraud","valid"])
start_date = st.sidebar.date_input("From", value = datetime.date( 2025, 12, 1 ) )
end_date = st.sidebar.date_input("To", value = datetime.date( 2025, 12, 31 ))
st.sidebar.markdown("---")
limit_rows = st.sidebar.slider("Max rows shown in table", min_value=50, max_value=1000, value=100, step=50)


# Numerical KPI
all = df['processed_at'].between(start_date, end_date)

# Percentage from previous day 
previous_day = df['processed_at']< start_date 
percentage_increase = ((previous_day - all)/all)*100




 # Setting the header and KPI
st.title("Near Time Transaction Monitoring Dashboard")

 # setting tabs
T1,T2 = st.tabs(["Overview", "Details"])

with T1:
    k1, k2, k3, k4, k5 = st.columns([2,2,2,2,2])
    k1.metric('All Transactions', f"{df['Age'].nunique()}","10%", chart_data = df['Age'],chart_type = 'line', border=True)
    k2.metric('Valid Transactions', f"{df['Age'].nunique()}" ,"10%", chart_data = df['Age'],chart_type = 'line', border=True)
    k3.metric('Fradulent Transactions', f"{df['Age'].nunique()}","10%", chart_data = df['Age'],chart_type = 'line', border=True)
    k4.metric('False Alarm', f"{df['Age'].nunique()}" ,"10%", chart_data = df['Age'],chart_type = 'line', border=True)
    k5.metric('Missed Fraud', f"{df['Age'].nunique()}","10%", chart_data = df['Age'],chart_type = 'line', border=True )
    st.markdown("---")
