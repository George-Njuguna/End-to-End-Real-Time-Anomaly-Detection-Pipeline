''' Build an app that:
1.	Lets user upload a CSV.
2.	Displays the head() of the DataFrame.
3.	Has a selectbox for choosing one column.
4.	Displays summary statistics of that column (df[column].describe()).
'''

import streamlit as st 
import pandas as pd
import seaborn as sns 
import matplotlib.pyplot as plt

# Get Streamlit theme colors
plt.rcParams["figure.facecolor"] = "none"
plt.rcParams["axes.facecolor"] = "none"

# Make all text white (works perfectly with Streamlit dark or light themes)
plt.rcParams["text.color"] = "white"
plt.rcParams["axes.labelcolor"] = "white"
plt.rcParams["xtick.color"] = "white"
plt.rcParams["ytick.color"] = "white"
plt.rcParams["legend.labelcolor"] = "white"
plt.rcParams["axes.titlecolor"] = "white"

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

    filter_by = st.selectbox("filter by",selected_columns)

    # Display selected data
    if selected_columns:
        st.write("### First 10 rows")
        st.dataframe(df[selected_columns].head(10))
        
        st.write("### Last 10 rows")
        st.dataframe(df[selected_columns].tail(10))
        
        st.write("### Statistical Description")
        st.dataframe(df[selected_columns].describe(include='all'))
    
     # Looping through all the columns 
    for col in selected_columns:
        if col != filter_by and filter_option == 'Numeric':
            fig, ax = plt.subplots(figsize=(8, 5))

            sns.lineplot(data= df, x=col, y=filter_by)
            ax.set_title(f"Line Plots with {filter_by} on Y-Axis")
            ax.legend(title='X-Axis Variable')
            plt.xticks(rotation=45, ha='right') 
            plt.tight_layout() 
            st.pyplot(fig)
            
        elif col != filter_by and (filter_option == 'Categorical' or filter_option == 'Boolean'):

            # Getting the contingency table 
            crosstab4 = pd.crosstab( df[col] , df[filter_by] ) 
            crosstab_4 = pd.crosstab( df[col] , df[filter_by] , normalize = 'index' ) * 100

            # Plotting the bar plot with the crosstab result
            fig , ax = plt.subplots( 1 , 2 ,figsize = ( 13, 6 ))
            for a in ax:
                a.set_facecolor("none")

            crosstab4.plot(kind='bar', stacked=False, ax = ax[0])
            crosstab_4.plot(kind='bar', stacked=False, ax = ax[1])

            ax[0].set_title( f'{filter_by} Count by {col}' , fontweight = 'bold')
            ax[0].set_ylabel( 'Count' , fontweight = 'bold')
            ax[0].set_xlabel(f'{col} Ownership' , fontweight = 'bold')

            ax[1].set_title( f'{filter_by} Rate by {col}' , fontweight = 'bold')
            ax[1].set_ylabel( f'{filter_by} Rate' , fontweight = 'bold')
            ax[1].set_xlabel(f'{col}' , fontweight = 'bold')

            # Annotating the bars 
            for p in ax[1].patches:
                height = p.get_height()
                
                ax[1].annotate(f'{height:.1f}%', 
                            (p.get_x() + p.get_width() / 2., height),  
                            ha='center', va='bottom')         
                     
            plt.tight_layout() 
            st.pyplot(fig)