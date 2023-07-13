import pandas as pd
import matplotlib.pyplot as plt
"""

# Load the CSV dataset using Pandas
data = pd.read_csv('partytotal.csv')


# Group the data by party and gender, and count the number of deputies
party_gender_counts = data.groupby(['partito', 'gender']).size().unstack().fillna(0)

# Sort parties based on the total number of deputies
party_gender_counts['Total'] = party_gender_counts.sum(axis=1)
party_gender_counts = party_gender_counts.sort_values(by='Total', ascending=False)
party_gender_counts = party_gender_counts.drop('Total', axis=1)

# Calculate the total number of deputies
total_deputies = party_gender_counts['male'] + party_gender_counts['female']

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
"""
import pandas as pd
import matplotlib.pyplot as plt

# Leggi il file CSV
df = pd.read_csv('ministrigender.csv')

# Seleziona le colonne pertinenti
df = df[['Governo', 'Ministro', 'gender']]

# Raggruppa per governo
gruppi_governo = df.groupby('Governo')

# Ciclo per creare grafici a bolle separati per ogni governo
for governo, dati_governo in gruppi_governo:
    # Conta le combinazioni di ruolo ministeriale e genere per il governo corrente
    combinazioni = dati_governo.groupby(['Ministro', 'gender']).size().reset_index(name='conteggio')

    # Dimensioni e colori delle bolle
    dimensioni = combinazioni['conteggio'] * 100  # Adatta le dimensioni delle bolle in base ai dati
    colori = ['blue' if g == 'M' else 'pink' for g in combinazioni['gender']]  # Assegna colori ai generi

    # Creazione del grafico a bolle
    fig, ax = plt.subplots()
    for i, row in combinazioni.iterrows():
        ruolo = row['Ministro']
        gender = row['gender']
        ax.scatter(i, ruolo, s=dimensioni[i], c=colori[i], alpha=0.7)
        ax.annotate(f'{gender}', (i, ruolo), ha='center', va='center')

    # Etichette degli assi
    ax.set_xticks(range(len(combinazioni)))
    ax.set_xticklabels(combinazioni['Ministro'], rotation=45, ha='right')
    ax.set_yticks(range(len(combinazioni['Ministro'].unique())))
    ax.set_yticklabels(combinazioni['Ministro'].unique())

    # Titolo e label degli assi
    plt.title(f'Combinazioni di ruolo ministeriale e genere - {governo}')
    plt.xlabel('Ruolo Ministeriale')
    plt.ylabel('Ruolo Ministeriale')

    # Mostra il grafico a bolle per il governo corrente
    plt.tight_layout()
    plt.show()
