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
    filter = st.multiselect("Choose datatype", ['Numeric','Categorical','All','Boolean'])

    if filter == "Numeric":
        cols = df.select_dtypes(include=['int','float']).columns.to_list()

    elif filter == 'Categorical':
        cols = df.select_dtypes(include=['object','category']).columns.to_list()

    elif filter == 'Boolean':
        cols = df.select_dtypes(include=['boolean']).columns.to_list()

    else:
        cols = df.columns.to_list()



    

