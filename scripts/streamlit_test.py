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
from scipy.stats import gaussian_kde
import plotly.graph_objects as go

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


def get_tot_avg_ammount(data, prev_day_filter, all_filter):
     # getting data counts that was before curr date 
    prev_day_data = data[prev_day_filter]
    previous_day_ammount = (prev_day_data["ammount"].sum())

     # getting data counts on the current date range
    curr_day_data = data[all_filter]   
    curr_day_ammount = (curr_day_data["ammount"].sum())
    
     # getting avg 
    avg = curr_day_ammount / (curr_day_data.shape)[0]

     # getting the percentages increase
    if previous_day_ammount == 0:
        perc = np.nan
        avg_perc = np.nan
    else:
        avg_prev = previous_day_ammount / (prev_day_data.shape)[0]
        perc = ((curr_day_ammount-previous_day_ammount)/previous_day_ammount)*100

        avg_perc = ((avg-avg_prev)/avg_prev)*100
    
    return curr_day_ammount, perc, avg, avg_perc 


def get_avg_max_risk(data, prev_day_filter, all_filter):
    filter = df['probability'] >= 75
     # getting data counts that was before curr date 
    prev_day_data = data[prev_day_filter]
    previous_day_sum = (prev_day_data["probability"].sum())
    prev_day_risk_sum = ((df[filter]).shape)[0]

     # getting data counts on the current date range
    curr_day_data = data[all_filter]   
    curr_day_sum = (curr_day_data["probability"].sum())
    max_risk = (curr_day_data["probability"].max())
    curr_day_fil_data = df[filter]
    curr_high_risk_sum = (curr_day_fil_data.shape)[0] 
    
     # getting avg 
    avg = curr_day_sum / (curr_day_data.shape)[0]

     # getting the percentages increase
    if previous_day_sum == 0:
        avg_perc = np.nan
        high_risk_perc = np.nan

    else:
        avg_prev = previous_day_sum / (prev_day_data.shape)[0]

        avg_perc = ((avg-avg_prev)/avg_prev)*100
        high_risk_perc =((curr_high_risk_sum - prev_day_risk_sum)/prev_day_risk_sum)*100
    
    return max_risk, avg, avg_perc, curr_high_risk_sum, high_risk_perc

def categorize(row):
    if row["fraud"] == 1:
        return "Fraud"
    elif row["fraud"] == 0 and row["prediction"] == 1:
        return "False Alarms"
    elif row["fraud"] == 1 and row["prediction"] == 0:
        return "Missed Alarms"
    else:
        return "Valid"
    
# Function to color text in a column
def row_text_color(row):
    color = COLOURS.get(row["category"], "black")
    return [f'color: {color}']*len(row)

 # Setting colours
COLOURS = {
    "All": "#0077B6",
    "Valid": "#47622B",
    "Fraud": "#D7520A",
    "Missed Alarms": "#E89528",
    "False Alarms" : "#174B4C"

}
    

 # Setting sidebar 
st.sidebar.title("Filters and Settings")


st.sidebar.markdown("---")
filter_trans = st.sidebar.selectbox("Transction Type",["Show All","All","Fraud","Valid"])
start_dt = st.sidebar.date_input("From", value = datetime.date( 2025, 12, 1 ) )
end_dt = st.sidebar.date_input("To", value = None)
st.sidebar.markdown("---")
limit_rows = st.sidebar.slider("Max rows shown in table", min_value=50, max_value=1000, value=100, step=50)

if start_dt is None:
    st.error("Start Date cannot be empty.")
elif start_dt is not None and end_dt is None: 
    start_date = pd.to_datetime(start_dt)
    end_date = start_date + pd.Timedelta(days=1)

    previous_date = start_date - pd.Timedelta(days=1)
    previous_day_filter =  (df['processed_at'] < start_date) & (df['processed_at'] >= previous_date)
    mess = "From Past day"
else:
    start_date = pd.to_datetime(start_dt)
    end_date = pd.to_datetime(end_dt)

    day_difference =(end_date - start_date).days

    if day_difference == 1:
        previous_date = start_date - pd.Timedelta(days=1)
        previous_day_filter = (df['processed_at'] < start_date) & (df['processed_at'] >= previous_date)
        mess = "From Past day"
    
    else:
        previous_date = start_date - pd.Timedelta(days=day_difference)
        previous_day_filter = (df['processed_at'] < start_date) & (df['processed_at'] >= previous_date)

        if day_difference == 7 :
            mess = "From Past Week"
        elif day_difference == 14 :
            mess = "From Past 2 Weeks"
        elif day_difference == 21:
            mess = "From Past 3 Weeks"
        elif day_difference == 31 or day_difference == 30:
            mess = "From Past Month"
        else:
            mess = f"From Past {day_difference} Days"



 # Transactions.
all_filter = df['processed_at'].between(start_date, end_date)
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

 # getting transacted ammount and average
amm_trans = get_tot_avg_ammount(df , previous_day_filter , all_filter)

total_transacted = amm_trans[0]
ammount_percentage = amm_trans[1]
avg_transaction = amm_trans[2]
avg_percentage = amm_trans[3]

risk = get_avg_max_risk(df , previous_day_filter , all_filter)

max_risk = risk[0]
avg_risk = risk[1]
avg_risk_perc = risk[2]
high_risk_sum = risk[3]
high_risk_perc = risk[4]

 # probabilities



 # Setting the header and KPI
st.title("Near Time Transaction Monitoring Dashboard")

 # setting tabs
T1,T2 = st.tabs(["Overview", "Details"])

with T1:
    k1, k2, k3, k4, k5 = st.columns([1.2,1.2,1,1,1])
    k1.metric("All Transactions", f"{all_transactions:,}", f"{all_percentages:.2f}% {mess}")
    k2.metric("Valid Transactions", f"{valid_transactions:,}", f"{valid_percentages:.2f}% {mess}")
    k3.metric("Fraudulent Transactions", f"{fraud_transactions:,}", f"{fraud_percentages:.2f}% {mess}")
    k4.metric("False Alarm", f"{false_alarm_transactions:,}", f"{false_alarm_percentages:.2f}% {mess}")
    k5.metric("Missed Fraud", f"{missed_alarm_transactions:,}", f"{missed_alarm_percentages:.2f}% {mess}")
    st.markdown("---")

    st.header("Overview")

    with st.container():

         # sums
        fraud_sum = df.loc[fraud_filter, 'ammount'].sum()
        valid_sum = df.loc[valid_filter, 'ammount'].sum()
        false_alarm_sum = df.loc[false_alarm_filter, 'ammount'].sum()
        missed_alarm_sum = df.loc[missed_alarm_filter, 'ammount'].sum()

        col1, col2 = st.columns([3, 1])

        all_hourly = df.resample("h", on="processed_at")["ammount"].sum().rolling(3).mean().rename("All")
        fraud_hourly = df[fraud_filter].resample("h", on="processed_at")["ammount"].sum().rolling(3).mean().rename("Fraud")
        valid_hourly = df[valid_filter].resample("h", on="processed_at")["ammount"].sum().rolling(3).mean().rename("Valid")
        false_alarm_hourly = df[false_alarm_filter].resample("h", on="processed_at")["ammount"].sum().rolling(3).mean().rename("False Alarms")
        missed_alarm_hourly = df[missed_alarm_filter].resample("h", on="processed_at")["ammount"].sum().rolling(3).mean().rename("Missed Alarms")

        df_hourly = pd.concat([all_hourly, fraud_hourly, valid_hourly, false_alarm_hourly, missed_alarm_hourly], axis=1).reset_index()


        if filter_trans == "Show All":
            
            df_melted = df_hourly.melt(
                id_vars="processed_at",
                value_vars=["All", "Valid", "Fraud"],
                var_name="category",
                value_name="amount"
            )
            labels = ["Valid", "Fraud", "False Alarms", "Missed Alarms"]
            values = [valid_sum, fraud_sum, false_alarm_sum, missed_alarm_sum]

        elif filter_trans == "All":
            
            df_melted = df_hourly[["processed_at", "All"]].rename(columns={"All": "amount"})
            labels = ["Valid", "Fraud", "False Alarms", "Missed Alarms"]
            values = [valid_sum, fraud_sum, false_alarm_sum, missed_alarm_sum]

        elif filter_trans == "Fraud":
            df_melted = df_hourly[["processed_at", "Fraud"]].rename(columns={"Fraud": "amount"})
            labels = ["Fraud", "Missed Alarms"]
            values = [fraud_sum, missed_alarm_sum]

        else:  # Valid
            df_melted = df_hourly[["processed_at", "Valid"]].rename(columns={"Valid": "amount"})
            labels = ["Valid", "False Alarms"]
            values = [valid_sum, false_alarm_sum]
        
         # line Chart
        with col1:
            with st.container(border=True):
                if filter_trans == "Show All":
                    fig = px.line(
                        df_melted,
                        x="processed_at",
                        y="amount",
                        color="category",
                        color_discrete_map=COLOURS,
                        line_shape="spline",
                        title="Valid, Fraud & Total Transactions Over Time"
                    )
                else:
                    fig = px.line(
                        df_melted,
                        x="processed_at",
                        y="amount",
                        line_shape="spline",
                        title=f"{filter_trans} Transactions Over Time",
                        color_discrete_sequence=[COLOURS.get(filter_trans, "#2196F3")]
                    )

                fig.update_traces(mode="lines+markers")
                st.plotly_chart(fig, width='stretch', theme="streamlit")

        with col2:
            pie_labels = ["Valid", "Fraud", "False Alarms", "Missed Alarms"]
            pie_values = [valid_sum, fraud_sum, false_alarm_sum, missed_alarm_sum]
            with st.container(border=True):
                fig = px.pie(
                    names=pie_labels,
                    values=pie_values,
                    hole=0.75,
                    color=pie_labels,
                    color_discrete_map=COLOURS   
                )

                fig.update_traces(textinfo='percent+label', rotation = 160) 
                fig.update_layout(title="Transactions Summary")

                st.plotly_chart(fig, width="stretch", theme="streamlit")

     # bar graphs

    with st.container(border=True):
        # Melt for stacked bars
        df_bar_melted = df_hourly.melt(
            id_vars="processed_at",
            value_vars=labels,
            var_name="category",
            value_name="amount"
        )

        fig_bar = px.bar(
            df_bar_melted,
            x="processed_at",
            y="amount",
            color="category",
            color_discrete_map=COLOURS,
            title="Transaction Amounts Over Time"
        )
        fig_bar.update_layout(
            barmode="stack",
            xaxis_title="Time",
            yaxis_title="Transaction Amount",
            legend_title="Category"
        )
        st.plotly_chart(fig_bar, width="stretch", theme="streamlit")

with T2:
    k1, k2, k3, k4, k5 = st.columns([1.2,1.2,1,1,1])
    k1.metric("Ammount Transacted", f"{total_transacted:,}$", f"{ammount_percentage:.2f}% {mess}")
    k2.metric("Average Transaction", f"{avg_transaction:.2f}$", f"{avg_percentage:.2f}% {mess}")
    k3.metric("Average Risk Score", f"{avg_risk:.2f}%", f"{avg_risk_perc:.2f}% {mess}")
    k4.metric("Max Risk Score", f"{max_risk:.2f}%")
    k5.metric("High Risk Transactions", f"{high_risk_sum:,}", f"{high_risk_perc:.2f}% {mess}")
    st.markdown("---")

    col1, col2 = st.columns([3, 1])

    # adding histogram 
    data = df[((df['processed_at'] >= start_date) & (df['processed_at'] >= start_date))]

    probs = data["probability"].dropna().values

    # KDE calculation
    kde = gaussian_kde(probs)
    x_range = np.linspace(probs.min(), probs.max(), 200)
    kde_values = kde(x_range)

    with st.container():
        with col1:
            with st.container(border=True):
                # Plotly figure
                fig = go.Figure()

                # Histogram
                fig.add_trace(go.Histogram(
                    x=probs,
                    histnorm='probability density',
                    opacity=0.5,
                    name="Histogram"
                ))

                # KDE curve
                fig.add_trace(go.Scatter(
                    x=x_range,
                    y=kde_values,
                    mode='lines',
                    name="KDE",
                    line=dict(width=3)
                ))

                fig.update_layout(
                    title="Probability Distribution",
                    xaxis_title="Probability",
                    yaxis_title="Density",
                    bargap=0.1
                )

                st.plotly_chart(fig, width="stretch", theme="streamlit")
        
        with col2:
            with st.container(border=True):
                fig = px.violin(
                    data,
                    x="probability",
                    box=True,
                    points="all",
                    title="Probability Distribution (Violin Plot)"
                )
                st.plotly_chart(fig, width="stretch", theme="streamlit")

    col3, col4, col5= st.columns([1,1,2])
    with st.container():
        with col3:
            with st.container(border=True):
                fig_valid_fraud = go.Figure()

                fig_valid_fraud.add_trace(go.Bar(
                    x=["Valid", "Fraud"],
                    width=[0.3, 0.3],
                    y=[valid_transactions, fraud_transactions],
                    text=[valid_transactions, fraud_transactions],
                    textposition="outside",
                    marker=dict(
                        line=dict(width=0),
                        opacity=0.85
                    )
                ))

                fig_valid_fraud.update_layout(
                    title="Valid vs Fraud Transactions",
                    xaxis_title="Transaction Type",
                    yaxis_title="Count",
                    bargap=0.3,
                    showlegend=False
                )

                st.plotly_chart(fig_valid_fraud, width="stretch", theme="streamlit")

        with col4:
            with st.container(border=True):
                fig_false_missed = go.Figure()

                fig_false_missed.add_trace(go.Bar(
                    x=["False Alarm", "Missed Fraud"],
                    width=[0.3, 0.3],
                    y=[false_alarm_transactions, missed_alarm_transactions],
                    text=[false_alarm_transactions, missed_alarm_transactions],
                    textposition="outside",
                    marker=dict(
                        line=dict(width=0),
                        opacity=0.85
                    )
                ))

                fig_false_missed.update_layout(
                    title="False Alarm vs Missed Fraud",
                    xaxis_title="Category",
                    yaxis_title="Count",
                    bargap=0.3,
                    showlegend=False
                )

                st.plotly_chart(fig_false_missed, width="stretch",theme="streamlit")

        with col5:
            with st.container():

                if filter_trans == "Show All" or filter_trans == "All":
                    df = df[all_filter]
                elif filter_trans == "Fraud":
                    df = df[false_alarm_filter | fraud_filter]
                else:
                    df = df[missed_alarm_filter | valid_filter]

                table_data = ((df.sort_values(by='processed_at',ascending = False )).reset_index()).head(limit_rows) 

                table_data["category"] = table_data.apply(categorize, axis=1)

                styled_df = table_data[["transaction_id", "processed_at", "ammount", "probability","category"]].style.apply(row_text_color, axis=1)
                st.header("Current Transactions")
                st.dataframe(styled_df, width="stretch")