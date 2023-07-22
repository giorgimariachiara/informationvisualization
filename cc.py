import plotly.graph_objects as go

# Dati di esempio per i presidenti
presidenti = [
    {
        "nome": "Giuseppe",
        "cognome": "Verdi",
        "genere": "male",
        "data_inizio_mandato": "01/01/2000",
        "data_fine_mandato": "31/12/2010",
        "legislatura": "XVII"
    },
    {
        "nome": "Maria",
        "cognome": "Rossi",
        "genere": "female",
        "data_inizio_mandato": "01/01/2006",
        "data_fine_mandato": "31/12/2010",
        "legislatura": "XVIII"
    },
    # Aggiungi gli altri presidenti qui...
]

# Estrai le informazioni dai dati
nomi_presidenti = [f"{presidente['nome']} {presidente['cognome']}" for presidente in presidenti]
generi = [presidente['genere'] for presidente in presidenti]
date_inizio_mandato = [presidente['data_inizio_mandato'] for presidente in presidenti]
date_fine_mandato = [presidente['data_fine_mandato'] for presidente in presidenti]
durate_mandato = [f"{inizio} - {fine}" for inizio, fine in zip(date_inizio_mandato, date_fine_mandato)]
legislature = [presidente['legislatura'] for presidente in presidenti]

# Calcola la durata del mandato in giorni
from datetime import datetime
durate_giorni = [(datetime.strptime(fine, "%d/%m/%Y") - datetime.strptime(inizio, "%d/%m/%Y")).days
                for inizio, fine in zip(date_inizio_mandato, date_fine_mandato)]

# Definisci i colori delle bolle in base al genere
colori = {'male': 'blue', 'female': 'pink'}
colori_presidenti = [colori[genere] for genere in generi]

# Crea la chart a bolle
fig = go.Figure()

for nome, colore, durata, legislatura in zip(nomi_presidenti, colori_presidenti, durate_giorni, legislature):
    fig.add_trace(go.Scatter(
        x=[durata],
        y=[nome],
        mode='markers',
        marker=dict(
            size=durata/100,  # Modifica la dimensione delle bolle in base alla durata del mandato
            color=colore,  # Imposta il colore della bolla in base al genere
            line=dict(width=2, color='black'),
        ),
        text=f"Legislatura: {legislatura}<br>Genere: {generi}<br>Durata: {durata} giorni",
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
