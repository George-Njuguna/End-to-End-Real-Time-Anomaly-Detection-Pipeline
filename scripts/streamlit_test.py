import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt



plt.rcParams["figure.facecolor"] = "none"
plt.rcParams["axes.facecolor"] = "none"

plt.rcParams["text.color"] = "white"
plt.rcParams["axes.labelcolor"] = "white"
plt.rcParams["xtick.color"] = "white"
plt.rcParams["ytick.color"] = "white"
plt.rcParams["legend.labelcolor"] = "white"
plt.rcParams["axes.titlecolor"] = "white"


st.title("Sales Dashboard")

# TOP FILTERS (COLUMNS)
c1, c2, c3 = st.columns(3)
with c1:
    st.selectbox("Year", [2022, 2023, 2024])
with c2:
    st.selectbox("Region", ["All", "East", "West"])
with c3:
    st.selectbox("Category", ["Electronics", "Food", "Clothing"])

# MAIN TABS
tab1, tab2, tab3 = st.tabs(["Summary", "Charts", "Raw Data"])

with tab1:
    st.write("KPIs go here")

with tab2:
    st.write("Graphs go here")

with tab3:
    st.write("Data table goes here")

# EXPANDER FOR ADVANCED OPTIONS
with st.expander("Advanced Settings"):
    st.write("Hidden configuration options here…")
