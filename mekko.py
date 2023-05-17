import pandas as pd
import plotly.graph_objects as go

# Read the CSV dataset using Pandas
data = pd.read_csv('your_dataset.csv')

# Calculate the percentage of deputies graduated by gender
gender_counts = data['Gender'].value_counts()
graduated_counts = data[data['Graduated'] == 'Yes']['Gender'].value_counts()
percentage_graduated = (graduated_counts / gender_counts) * 100

# Create the Mekko bar chart using Plotly
fig = go.Figure(data=[go.Bar(
    x=percentage_graduated.index,
    y=percentage_graduated.values,
    marker=dict(color=['blue', 'pink']),  # Specify colors for each gender
)])

# Customize the chart layout
fig.update_layout(
    title='Percentage of Deputies Graduated by Gender',
    xaxis=dict(title='Gender'),
    yaxis=dict(title='Percentage')
)

# Display the chart
fig.show()
