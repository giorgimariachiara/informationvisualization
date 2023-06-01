import pandas as pd
import plotly.graph_objects as go

# Read the CSV dataset using Pandas
data = pd.read_csv('graduation.csv')

# Calculate the total number of deputies by gender
gender_counts = data['gender'].value_counts()

# Calculate the number of graduated and non-graduated deputies by gender
graduated_counts = data[data['graduated'] == 'yes']['gender'].value_counts()
non_graduated_counts = gender_counts - graduated_counts

# Calculate the percentage of graduated deputies by gender
percentage_graduated = (graduated_counts / gender_counts) * 100

# Create the Mekko bar chart using Plotly
fig = go.Figure(data=[
    go.Bar(
        x=graduated_counts.index,
        y=graduated_counts.values,
        name='Graduated',
        marker=dict(color=['green', 'purple']),  # Set blue for male and red for female
        text=percentage_graduated.values.round(2),
        textposition='auto',
        texttemplate='%{text:.2f}%',
        hovertemplate='%{y} Graduated<br>%{text} of Total<br>',
    ),
    go.Bar(
        x=non_graduated_counts.index,
        y=non_graduated_counts.values,
        name='Non-Graduated',
        marker=dict(color='lightgrey'),  # Set light grey for non-graduated
        hovertemplate='%{y} Non-Graduated<br>',
    )
])

# Customize the chart layout
fig.update_layout(
    title='Proportion of Graduated and Non-Graduated Deputies by Gender',
    xaxis=dict(title='Gender'),
    yaxis=dict(title='Number of Deputies'),
    barmode='stack',
    legend=dict(title='Status'),
)

# Display the chart
fig.show()
