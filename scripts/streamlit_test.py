import streamlit as st 

st.title("Hello, Streamlit!")
st.header("DEEZ NUTZ")
st.write("Welcome to your first interactive app.")
name = st.text_input("input your age:")

if st.button("Age counter"):
    age = int(name)+10
    st.write(f"Hello, your age will be {age} in 2035")