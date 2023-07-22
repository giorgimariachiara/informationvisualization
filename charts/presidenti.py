import pandas as pd
import plotly.express as px

# Carica i dati dal file CSV
df = pd.read_csv('presidenti.csv')

# Calcola la somma dei mandati per ogni presidente
presidenti_mandati = df.groupby('nome')['mandato'].sum().reset_index()

# Lista dei colori personalizzati
colors = [
    "#A9FF38", "#98E533", "#84C72C", "#71AA25", "#649820", "#56821C", "#34861F", "#40A027", "#2C7419",
    "#2C8F60", "#237B51", "#27CA7C", "#12CD73", "#529E79", "#9ACD32", "#063C25", "#04673D", "#8CC3AC",
    "#A8EBCF", "#66B17C", "#1BAC46", "#18933D", "#0D8030", "#064E1C", "#2C643D", "#688771", "#2DA92A",
    "#44801B", "#5DC815", "#34531F", "#CC2210"
]

# Imposta il colore rosso per Giorgia Meloni
giorgia_meloni_index = presidenti_mandati[presidenti_mandati['nome'] == 'GIORGIA MELONI'].index[0]
colors[giorgia_meloni_index] = "#FF0000"

# Imposta il colore verde per Silvio Berlusconi
silvio_berlusconi_index = presidenti_mandati[presidenti_mandati['nome'] == 'SILVIO BERLUSCONI'].index[0]
colors[silvio_berlusconi_index] = "#A9FF38"

# Crea la pie chart interattiva con popup di informazioni
fig = px.pie(
    presidenti_mandati,
    values='mandato',
    names='nome',
    title='Presidenti del consiglio italiani',
    hover_data=['mandato'],
    hole=0.5,
)

# Imposta i colori personalizzati
fig.update_traces(marker=dict(colors=colors))

# Imposta il pull solo per la fetta di Giorgia Meloni per farla uscire
pull_values = [0.1 if i == giorgia_meloni_index else 0 for i in range(len(presidenti_mandati))]
fig.update_traces(pull=pull_values)

# Aggiungi popup di informazioni al passaggio del mouse
fig.update_traces(textinfo='none')

# Mostra la pie chart
fig.show()
