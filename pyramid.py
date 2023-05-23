import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV dataset using Pandas
data = pd.read_csv('party.csv')

# Group the data by party and gender, and count the number of deputies
party_gender_counts = data.groupby(['gruppoPar', 'gender']).size().unstack().fillna(0)

# Sort parties based on the total number of deputies
party_gender_counts['Total'] = party_gender_counts.sum(axis=1)
party_gender_counts = party_gender_counts.sort_values(by='Total', ascending=False)
party_gender_counts = party_gender_counts.drop('Total', axis=1)

# Calculate the total number of deputies
total_deputies = party_gender_counts['male'] + party_gender_counts['female']
"""
# Calculate the percentage of male and female deputies
percentage_male = party_gender_counts['male'] / total_deputies * 100
percentage_female = party_gender_counts['female'] / total_deputies * 100

# Create a pyramid graph
fig, ax = plt.subplots()
"""

# Filtra i partiti con meno di 10 deputati
party_gender_counts = party_gender_counts[total_deputies >= 10]

# Ricalcola le percentuali di genere e i totali dei deputati
total_deputies = party_gender_counts['male'] + party_gender_counts['female']
percentage_male = party_gender_counts['male'] / total_deputies * 100
percentage_female = party_gender_counts['female'] / total_deputies * 100

# Creazione del grafico a piramide
fig, ax = plt.subplots()


# Set the bar width
bar_width = 0.9

# Set the position of the bars on the y-axis
y_pos = range(len(party_gender_counts))

# Plot male deputies
ax.barh(y_pos, percentage_male, height=bar_width, color='b', align='center', label='Male')

# Plot female deputies
ax.barh(y_pos, -percentage_female, height=bar_width, color='r', align='center', label='Female')

# Add party labels to the y-axis
ax.set_yticks(y_pos)
ax.set_yticklabels(party_gender_counts.index)

ax.tick_params(axis='y', pad=5)
ax.yaxis.set_tick_params(rotation=0, labelsize=7)

# Imposta lo spazio aggiuntivo sopra e sotto le label
label_margin = 0.1
ax.set_yticks(y_pos)
labels = ax.set_yticklabels(party_gender_counts.index)

# Regola lo spazio tra le label
for label in labels:
    label.set_bbox(dict(pad=label_margin, facecolor='white', edgecolor='none'))


# Add labels, title, and legend
ax.set_xlabel('Percentage of Deputies')
ax.set_title('Political Party Distribution of Deputies by Gender')
ax.legend(title='Gender')

# Invert the y-axis
ax.invert_yaxis()

# Show the plot
plt.tight_layout()
plt.show()
