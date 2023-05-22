import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load the CSV dataset using Pandas
data = pd.read_csv('party.csv')

# Group the data by party and gender, and count the number of deputies
party_gender_counts = data.groupby(['gruppoPar', 'gender']).size().unstack().fillna(0)

# Sort parties based on the total number of deputies
party_gender_counts['Total'] = party_gender_counts.sum(axis=1)
party_gender_counts = party_gender_counts.sort_values(by='Total', ascending=False)
party_gender_counts = party_gender_counts.drop('Total', axis=1)

# Create a population pyramid plot
fig, ax = plt.subplots()

# Set the bar width
bar_width = 0.4

# Set the position of the bars on the y-axis
y_pos = np.arange(len(party_gender_counts))

# Plot male deputies
ax.barh(y_pos, party_gender_counts['male'], height=bar_width, color='b', align='center', label='male')

# Plot female deputies
ax.barh(y_pos, -party_gender_counts['female'], height=bar_width, color='r', align='center', label='vemale')

# Add party labels to the y-axis
ax.set_yticks(y_pos)
ax.set_yticklabels(party_gender_counts.index)

# Add labels, title, and legend
ax.set_xlabel('Number of Deputies')
ax.set_title('Political Party Distribution of Deputies by Gender')
ax.legend(title='Gender')

# Invert the y-axis
ax.invert_yaxis()

# Show the plot
plt.show()
