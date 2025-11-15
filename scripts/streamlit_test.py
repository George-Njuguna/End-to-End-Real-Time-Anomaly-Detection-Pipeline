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


st.title("UPLOAD CSV HERE")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    # Datatype filter
    filter_option = st.selectbox("Choose datatype", ["All", "Numeric", "Categorical", "Boolean"])

    # Filter columns
    if filter_option == "Numeric":
        cols = df.select_dtypes(include=["int", "float"]).columns.to_list()
    elif filter_option == "Categorical":
        cols = df.select_dtypes(include=["object", "category"]).columns.to_list()
    elif filter_option == "Boolean":
        cols = df.select_dtypes(include=["bool"]).columns.to_list()
    else:
        cols = df.columns.to_list()

    # Multiselect for visible columns
    selected_columns = st.multiselect(
        "Choose columns",
        cols,
        default=cols,
        key=f"cols_{filter_option}"
    )

    # Choose a Y-axis / filter column
    filter_by = st.selectbox("filter by", selected_columns)

    # Display table info
    if selected_columns:
        st.write("### First 10 rows")
        st.dataframe(df[selected_columns].head(10))

        st.write("### Last 10 rows")
        st.dataframe(df[selected_columns].tail(10))

        st.write("### Statistical Description")
        st.dataframe(df[selected_columns].describe(include="all"))


    for col in selected_columns:
        if col == filter_by:
            continue


        if filter_option == "Numeric":
            fig, ax = plt.subplots(figsize=(8, 5))

            sns.lineplot(data=df, x=col, y=filter_by, ax=ax)

            ax.set_title(f"{col} vs {filter_by}", fontsize=14, fontweight="bold")
            plt.xticks(rotation=45)

            st.pyplot(fig)


        else:
            crosstab_raw = pd.crosstab(df[col], df[filter_by])
            crosstab_pct = pd.crosstab(df[col], df[filter_by], normalize="index") * 100

            fig, ax = plt.subplots(1, 2, figsize=(14, 6))

            # Raw counts barplot
            crosstab_raw.plot(kind="bar", ax=ax[0])
            ax[0].set_title(f"{filter_by} Count by {col}", fontweight="bold")
            ax[0].set_xlabel(col)
            ax[0].set_ylabel("Count")
            ax[0].tick_params(axis="x", rotation=45)

            # Percent barplot
            crosstab_pct.plot(kind="bar", ax=ax[1])
            ax[1].set_title(f"{filter_by} Rate by {col}", fontweight="bold")
            ax[1].set_xlabel(col)
            ax[1].set_ylabel("Rate (%)")
            ax[1].tick_params(axis="x", rotation=45)

            # Annotate 
            for p in ax[1].patches:
                height = p.get_height()
                ax[1].annotate(
                    f"{height:.1f}%",
                    (p.get_x() + p.get_width() / 2, height),
                    ha="center",
                    va="bottom",
                    fontsize=9,
                    color="white"       # annotation is white
                )

            plt.tight_layout()
            st.pyplot(fig)
