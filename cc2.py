import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import locale

# Leggi il CSV
df = pd.read_csv("presidenticamera.csv")

# Imposta la lingua locale italiana per il parsing delle date
locale.setlocale(locale.LC_TIME, 'it_IT.utf8')

# Converte le colonne "Inizio" e "Fine" nel formato "dd/mm/yyyy"
df['Inizio'] = df['Inizio'].apply(lambda x: datetime.strptime(x, '%d %B %Y').strftime('%d/%m/%Y'))
df['Fine'] = df['Fine'].apply(lambda x: datetime.today().strftime('%d/%m/%Y') if x == "in carica" else datetime.strptime(x, '%d %B %Y').strftime('%d/%m/%Y'))

# Trasforma il DataFrame in una lista di dizionari
presidenti = df.to_dict(orient='records')

# Estrai le informazioni dai dati
nomi_presidenti = [f"{presidente['Nome Presidente']} {presidente['Cognome Presidente']}" for presidente in presidenti]
generi = [presidente['Gender'] for presidente in presidenti]
date_inizio_mandato = [presidente['Inizio'] for presidente in presidenti]
date_fine_mandato = [presidente['Fine'] for presidente in presidenti]
durate_mandato = [(datetime.strptime(fine, "%d/%m/%Y") - datetime.strptime(inizio, "%d/%m/%Y")).days + 1
                for inizio, fine in zip(date_inizio_mandato, date_fine_mandato)]
legislature = [presidente['Legislatura'] for presidente in presidenti]

# Definisci i colori delle barre in base al genere
colori = {'male': 'blue', 'female': 'pink'}
colori_presidenti = [colori[genere] for genere in generi]

# Crea la Gantt chart con barre separate
fig = go.Figure()

for nome, colore, inizio, durata, legislatura in zip(nomi_presidenti, colori_presidenti, date_inizio_mandato, durate_mandato, legislature):
    fig.add_trace(go.Bar(
        x=[nome],
        y=[1],  # L'asse y è uguale per tutte le barre per evitare sovrapposizioni
        orientation='h',
        marker=dict(color=colore),
        width=durate_mandato * 86400000,  # Converti la durata in millisecondi
        text=f"Legislatura: {legislatura}<br>Genere: {generi}<br>Durata: {durata} giorni",
        hoverinfo='text',
    ))

# Imposta il layout della chart
fig.update_layout(
    title='Presidenti della Camera dei Deputati - Gantt Chart',
    xaxis=dict(title='Presidente'),
    yaxis=dict(title='Periodo Mandato', type='category'),
    showlegend=False,
    hovermode='closest',
)

# Mostra la chart
fig.show()
