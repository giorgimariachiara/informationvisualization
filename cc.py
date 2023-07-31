import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import locale


"""
# Imposta la lingua locale italiana
locale.setlocale(locale.LC_TIME, 'it_IT.utf8')

# Leggi il CSV
df = pd.read_csv("presidenticamera.csv")

# Converti le colonne "Inizio" e "Fine" nel formato "dd/mm/yyyy"
df['Inizio'] = df['Inizio'].apply(lambda x: datetime.strptime(x, '%d %B %Y').strftime('%d/%m/%Y'))

# Gestisci il caso in cui il valore sia "in carica"
df['Fine'] = df['Fine'].apply(lambda x: datetime.today().strftime('%d/%m/%Y') if x == "in carica" else datetime.strptime(x, '%d %B %Y').strftime('%d/%m/%Y'))

# Trasforma il DataFrame in una lista di dizionari
presidenti = df.to_dict(orient='records')

# Estrai le informazioni dai dati
nomi_presidenti = [f"{presidente['Nome Presidente']} {presidente['Cognome Presidente']}" for presidente in presidenti]
generi = [presidente['Gender'] for presidente in presidenti]
date_inizio_mandato = [presidente['Inizio'] for presidente in presidenti]
date_fine_mandato = [presidente['Fine'] for presidente in presidenti]
durate_mandato = [f"{inizio} - {fine}" for inizio, fine in zip(date_inizio_mandato, date_fine_mandato)]
#legislature = [presidente['Legislatura'] for presidente in presidenti]

# Calcola la durata del mandato in giorni
from datetime import datetime
durate_giorni = [(datetime.strptime(fine, "%d/%m/%Y") - datetime.strptime(inizio, "%d/%m/%Y")).days
                for inizio, fine in zip(date_inizio_mandato, date_fine_mandato)]

# Definisci i colori delle bolle in base al genere
colori = {'male': 'blue', 'female': 'pink'}
colori_presidenti = [colori[genere] for genere in generi]

# Crea la chart a bolle
fig = go.Figure()

# Set the constant to adjust the marker size difference
size_scaling_factor = 5  # You can adjust this value according to your preference

for i, nome, colore, durata, legislatura in zip(range(len(presidenti)), nomi_presidenti, colori_presidenti, durate_giorni, legislature):
    fig.add_trace(go.Scatter(
        x=[durata],  # Ora l'asse x rappresenta la durata del mandato in giorni
        y=[nome],
        mode='markers',
        marker=dict(
            size=max(5, size_scaling_factor * (1 + durata / 100)),  # Use a logarithmic scaling function for the marker size
            symbol='circle',  # Usa cerchi come simbolo per i marker
            color=colore,  # Imposta il colore della bolla in base al genere
            line=dict(width=2, color='black'),
        ),
        text=f"Legislatura: {legislatura}<br>Genere: {generi[i]}<br>Durata: {durata} giorni",
        hoverinfo='text',
    ))

# Imposta il layout della chart
fig.update_layout(
    title='Presidenti della Camera dei Deputati',
    xaxis=dict(title='Durata del mandato (giorni)'),  # Etichetta dell'asse x
    yaxis=dict(title='Presidente'),
    showlegend=False,
    hovermode='closest',
)

# Mostra la chart
fig.show()
"""
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import locale

# Imposta la lingua locale italiana
locale.setlocale(locale.LC_TIME, 'it_IT.utf8')

# Leggi il CSV
df = pd.read_csv("presidenticamera.csv")

# Converti le colonne "Inizio" e "Fine" nel formato "dd/mm/yyyy"
df['Inizio'] = df['Inizio'].apply(lambda x: datetime.strptime(x, '%d %B %Y').strftime('%d/%m/%Y'))

# Gestisci il caso in cui il valore sia "in carica"
df['Fine'] = df['Fine'].apply(lambda x: datetime.today().strftime('%d/%m/%Y') if x == "in carica" else datetime.strptime(x, '%d %B %Y').strftime('%d/%m/%Y'))

# Trasforma il DataFrame in una lista di dizionari
presidenti = df.to_dict(orient='records')

# Estrai le informazioni dai dati
nomi_presidenti = [f"{presidente['Nome Presidente']} {presidente['Cognome Presidente']}" for presidente in presidenti]
generi = [presidente['Gender'] for presidente in presidenti]
date_inizio_mandato = [presidente['Inizio'] for presidente in presidenti]
date_fine_mandato = [presidente['Fine'] for presidente in presidenti]

# Calcola la durata del mandato in giorni
durate_giorni = [(datetime.strptime(fine, "%d/%m/%Y") - datetime.strptime(inizio, "%d/%m/%Y")).days
                for inizio, fine in zip(date_inizio_mandato, date_fine_mandato)]

# Definisci i colori delle bolle in base al genere (pastello)
colori = {'male': 'rgba(150, 191, 228, 1)',  # Blue pastello con trasparenza 0.7
          'female': 'rgba(246, 173, 210, 0.7)'}  # Pink pastello con trasparenza 0.7
colori_presidenti = [colori[genere] for genere in generi]

# Crea la chart a bolle
fig = go.Figure()

# Set the constant to adjust the marker size difference
size_scaling_factor = 2

for i, nome, colore, durata in zip(range(len(presidenti)), nomi_presidenti, colori_presidenti, durate_giorni):
    fig.add_trace(go.Scatter(
        x=[durata],
        y=[nome],
        mode='markers',
        marker=dict(
            size=max(5, size_scaling_factor * (1 + durata / 100)),
            symbol='circle',
            color=colore,
            line=dict(width=2, color='black'),
        ),
        text=f"Genere: {generi[i]}<br>Durata: {durata} giorni",
        hoverinfo='text',
    ))

# Imposta il layout della chart
fig.update_layout(
    title='Presidenti della Camera dei Deputati',
    xaxis=dict(title='Durata del mandato (giorni)'),
    yaxis=dict(title='Presidente'),
    showlegend=False,
    hovermode='closest',
)

# Mostra la chart
fig.show()

