import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file
df = pd.read_csv('totaledeputati.csv')

# Count the number of deputies by gender
gender_counts = df['gender'].value_counts()

colors = ['green', 'purple']

# Create a bar chart
plt.bar(gender_counts.index, gender_counts.values, color=colors)

# Set the chart title and labels
plt.title('Number of Deputies by Gender')
plt.xlabel('Gender')
plt.ylabel('Count')

# Show the chart
plt.show()



# bar chart totale deputati maschi e femmine presenti in generale nella camera