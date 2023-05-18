import pandas as pd
import plotly.graph_objects as go

# Read the CSV dataset using Pandas
data1 = pd.read_csv('womengraduation.csv')
data2 = pd.read_csv('mengraduation.csv')

col-somma = data1[]

# Calculate the percentage of deputies graduated by gender
gender_counts = data['gender'].value_counts()
graduated_counts = data[data['graduated'] == 'yes']['gender'].value_counts()
percentage_graduated = (graduated_counts / gender_counts) * 100

# Create the Mekko bar chart using Plotly
fig = go.Figure(data=[go.Bar(
    x=percentage_graduated.index,
    y=percentage_graduated.values,
    marker=dict(color=['blue', 'pink']),
    text=percentage_graduated.values.round(2),  # Display the percentage as text on the bars
    textposition='auto',  # Position the text automatically
    texttemplate='%{text:.2f}%',  # Format the text as percentage with 2 decimal places
)])

# Customize the chart layout
fig.update_layout(
    title='Percentage of Deputies Graduated by Gender',
    xaxis=dict(title='Gender'),
    yaxis=dict(title='Percentage'),
    barmode='stack',  # Display bars in stacked mode
)

# Display the chart
fig.show()




