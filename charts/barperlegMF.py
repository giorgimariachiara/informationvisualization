import pandas as pd
import matplotlib.pyplot as plt

# Carica i dati dal file CSV
data = pd.read_csv('totaledeputatiperlegislatura.csv')

# Calcola il totale dei deputati per genere e legislatura
gender_legislature_counts = data.groupby(['legislatura', 'gender']).size().unstack()

# Crea la bar chart con colori specifici per genere
ax = gender_legislature_counts.plot(kind='bar', stacked=True, color=['purple', 'green'])

# Aggiungi etichette
plt.xlabel('Legislatura')
plt.ylabel('Numero Deputati')
plt.title('Totale Deputati per Genere e Legislatura')

# Modifica le etichette delle legislature
legislature_labels = ['Legislatura Costituente', 'Legislatura I', 'Legislatura II', 'Legislatura III', 'Legislatura IV', 'Legislatura V', 'Legislatura VI', 'Legislatura VII', 'Legislatura VIII', 'Legislatura IX', 'Legislatura X', 'Legislatura XI', 'Legislatura XII', 'Legislatura XIII', 'Legislatura XIV', 'Legislatura XV', 'Legislatura XVI', 'Legislatura XVII', 'Legislatura XVIII', 'Legislatura IX']  # Lista delle etichette desiderate
ax.set_xticklabels(legislature_labels)

# Mostra la chart
plt.show()


#barchart divisione deputati male female per legislatura