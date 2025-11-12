'''Build an app that:
1.	Lets user upload a CSV.
2.	Displays the head() of the DataFrame.
3.	Has a selectbox for choosing one column.
4.	Displays summary statistics of that column (df[column].describe()).
'''

import streamlit as st 
import pandas as pd

st.title('UPLOAD CV HERE')
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")


if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    columns = st.multiselect("Choose columns", df.columns.to_list())
    if columns:
        st.write('first 10 rows')
        st.dataframe(df[columns].head(10))
        st.write('last 10 colums')
        st.dataframe(df[columns].tail(10))
        st.dataframe(df[columns].describe(include=['bool','object']))
        


