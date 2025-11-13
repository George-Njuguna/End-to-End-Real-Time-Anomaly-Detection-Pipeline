'''Build an app that:
1.	Lets user upload a CSV.
2.	Displays the head() of the DataFrame.
3.	Has a selectbox for choosing one column.
4.	Displays summary statistics of that column (df[column].describe()).
'''

import streamlit as st 
import pandas as pd

st.title('UPLOAD CSV HERE')
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    # Single-choice filter
    filter_option = st.selectbox("Choose datatype", ['All','Numeric','Categorical','Boolean'])

    # Filter columns by data type
    if filter_option == "Numeric":
        cols = df.select_dtypes(include=['int', 'float']).columns.to_list()
    elif filter_option == 'Categorical':
        cols = df.select_dtypes(include=['object', 'category']).columns.to_list()
    elif filter_option == 'Boolean':
        cols = df.select_dtypes(include=['bool']).columns.to_list()
    else:
        cols = df.columns.to_list()

    # Choose columns (multiselect)
    selected_columns = st.multiselect(
        "Choose columns",
        cols,
        default=cols,
        key=f"cols_{filter_option}"  # forces refresh when filter changes
    )

    # Display selected data
    if selected_columns:
        st.write("### First 10 rows")
        st.dataframe(df[selected_columns].head(10))
        
        st.write("### Last 10 rows")
        st.dataframe(df[selected_columns].tail(10))
        
        st.write("### Statistical Description")
        st.dataframe(df[selected_columns].describe(include='all'))
