import pandas as pd
import plotly.graph_objects as go

# Read the CSV dataset using Pandas
data = pd.read_csv('graduation.csv')

# Calculate the total number of deputies
total_deputies = len(data)

# Calculate the graduated counts by gender
graduated_counts = data[data['graduated'] == 'yes']['gender'].value_counts()

# Calculate the percentage of deputies graduated by gender
percentage_graduatedFEM = (graduated_counts['female'] / total_deputies) * 100
percentage_graduatedMA = (graduated_counts['male'] / total_deputies) * 100

# Calculate the percentage of non-graduated deputies by gender
percentage_non_graduatedFEM = ((total_deputies - graduated_counts['female']) / total_deputies) * 100
percentage_non_graduatedMA = ((total_deputies - graduated_counts['male']) / total_deputies) * 100

# Create the bar chart using Plotly
fig = go.Figure(data=[
    go.Bar(
        x=['female'],
        y=[percentage_graduatedFEM],
        marker=dict(color='pink'),
        name='Graduated',
        text=percentage_graduatedFEM.round(2),
        textposition='auto',
        texttemplate='%{text:.2f}%',
    ),
    go.Bar(
        x=['female'],
        y=[percentage_non_graduatedFEM],
        marker=dict(color='lightpink'),
        name='Non-Graduated',
        text=percentage_non_graduatedFEM.round(2),
        textposition='auto',
        texttemplate='%{text:.2f}%',
    ),
    go.Bar(
        x=['male'],
        y=[percentage_graduatedMA],
        marker=dict(color='blue'),
        name='Graduated',
        text=percentage_graduatedMA.round(2),
        textposition='auto',
        texttemplate='%{text:.2f}%',
    ),
    go.Bar(
        x=['male'],
        y=[percentage_non_graduatedMA],
        marker=dict(color='lightblue'),
        name='Non-Graduated',
        text=percentage_non_graduatedMA.round(2),
        textposition='auto',
        texttemplate='%{text:.2f}%',
    ),
])

# Customize the chart layout
fig.update_layout(
    title='Percentage of Deputies Graduated by Gender',
    xaxis=dict(title='Gender'),
    yaxis=dict(title='Percentage'),
    barmode='stack',
)

# Display the chart
fig.show()
