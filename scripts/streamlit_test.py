import streamlit as st 

import streamlit as st

st.title("User Profile Form")

name = st.text_input("Enter your name:")
age = st.number_input("Enter your age:", min_value=1, max_value=120)
favorite_color = st.selectbox("Choose your favorite color:", ["Red", "Green", "Blue", "Yellow"])
hobbies = st.multiselect("Select your hobbies:", ["Reading", "Coding", "Music", "Traveling"])
rating = st.slider("How much do you like Streamlit?", 1, 10)

submitted = st.button("Submit")

if submitted:
    st.write("### Profile Summary")
    st.write(f"Name: {name}")
    st.write(f"Age: {age}")
    st.write(f"Favorite Color: {favorite_color}")
    st.write(f"Hobbies: {', '.join(hobbies) if hobbies else 'None'}")
    st.write(f"Streamlit Love Score: {rating}/10")
