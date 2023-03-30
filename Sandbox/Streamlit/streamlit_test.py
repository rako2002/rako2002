import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Create a title and a subtitle
st.title("Streamlit Playground")
st.subheader("Data Analysis")

# Load some example data
data = pd.read_csv("C:\Programs\Sandbox\sample_data.csv")

# Show the data in a table
st.write("Here's our example data:")
st.write(data)

# Show a histogram of a numerical variable
st.subheader("Histogram of Numerical Data")
hist_var = st.selectbox("Which numerical variable?", data.columns)
#hist_bins = st.slider("Number of bins:", 5, 50, 20)
#st.hist(data[hist_var], bins=hist_bins)


# Add a slider to the sidebar for selecting the number of bins
hist_bins = st.slider("Number of bins", min_value=1, max_value=50, value=20)


# Show the histogram using Matplotlib
fig, ax = plt.subplots()
ax.hist(data, bins=hist_bins)
ax.set_xlabel("Data Value")
ax.set_ylabel("Frequency")
st.pyplot(fig)


# Show a bar chart of categorical data
st.subheader("Bar Chart of Categorical Data")
cat_var = st.selectbox("Which categorical variable?", data.columns)
cat_counts = data[cat_var].value_counts()
st.bar_chart(cat_counts)

# Show a scatter plot of two numerical variables
st.subheader("Scatter Plot of Numerical Data")
x_var = st.selectbox("Which variable on the x-axis?", data.columns)
y_var = st.selectbox("Which variable on the y-axis?", data.columns)

fig2, ax2 = plt.subplots()
ax2.set_xlabel(x_var)
ax2.set_ylabel(y_var)
ax2.scatter(data[x_var], data[y_var])
st.pyplot(fig2)

# Show some descriptive statistics
st.subheader("Descriptive Statistics")
st.write(data.describe())

# Show a map of some location data
st.subheader("Map of Location Data")
lat = st.number_input("Latitude:")
lon = st.number_input("Longitude:")
zoom = st.slider("Zoom level:", 1, 15, 10)
st.map(pd.DataFrame({"lat": [lat], "lon": [lon]}), zoom=zoom)
