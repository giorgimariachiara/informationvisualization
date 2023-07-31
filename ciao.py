import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd
from sparql_dataframe import get
import requests
import numpy as np

endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)

#QUERY NUMERO TOTALE DONNE

totale_donne = """
SELECT DISTINCT ?persona ?cognome ?nome
?dataNascita ?luogoNascita "female" as ?gender 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

?d a ocd:deputato; 
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.
OPTIONAL{?d dc:description ?info}

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.
}}"""
df_totale_donne = sparql_dataframe.get(endpoint, totale_donne)
df_totale_donne = df_totale_donne[['nome', 'cognome', 'gender', 'persona']]

#QUERY NUMERO TOTALE UOMINI 5204 
totale_uomini = """
SELECT DISTINCT ?persona ?cognome ?nome
?dataNascita ?luogoNascita "male" as ?gender 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

?d a ocd:deputato; 
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.
OPTIONAL{?d dc:description ?info}

##anagrafica
?d foaf:surname ?cognome; foaf:gender "male" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.
}}"""
df_totale_uomini = sparql_dataframe.get(endpoint, totale_uomini)
df_totale_uomini = df_totale_uomini[['persona','nome', 'cognome', 'gender']]
df_totale_uomini.to_csv("totaledeputatieeeeee.csv",  index=False, index_label=False)

df_totale = pd.concat([df_totale_uomini, df_totale_donne])


#print(df_totale)

import pandas as pd

# URL della pagina Wikipedia
url = 'https://it.wikipedia.org/wiki/Presidenti_della_Camera_dei_deputati_(Italia)'

# Leggi tutte le tabelle dalla pagina
tables = pd.read_html(url)

# Seleziona la terza tabella
third_table = tables[3]

# Seleziona la quarta tabella
fourth_table = tables[4]

# Mostra la terza tabella
print("Terza tabella:")
third_table = third_table[[('Presidente', 'Presidente.2'), ('Mandato', 'Inizio'), ('Mandato', 'Fine')]]

# Mostra la quarta tabella
print("Quarta tabella:")
fourth_table = fourth_table[[('Presidente', 'Presidente.2'), ('Mandato', 'Inizio'), ('Mandato', 'Fine')]]

# Combina le due tabelle
combined_table = pd.concat([third_table, fourth_table], ignore_index=True)

# Estrai solo i nomi e cognomi senza date utilizzando espressioni regolari
pattern = r"([A-Za-z\s-]+)"
nomi = combined_table['Presidente', 'Presidente.2'].str.extract(pattern, expand=False)
nomi = nomi.drop_duplicates()
nomi = nomi.rename("Presidenti Camera")

presidenti_camera_dei_deputati = pd.DataFrame(nomi, columns=['Presidenti Camera'])

# Rimuovi la parte "Mandato" dalle colonne 'Inizio' e 'Fine'
combined_table[('Mandato', 'Inizio')] = combined_table[('Mandato', 'Inizio')].str.replace(r'\[.*\]', '', regex=True)
combined_table[('Mandato', 'Fine')] = combined_table[('Mandato', 'Fine')].str.replace(r'\[.*\]', '', regex=True)

# Seleziona solo le colonne desiderate per il DataFrame finale
presidenti_camera_dei_deputati['Inizio'] = combined_table[('Mandato', 'Inizio')]
presidenti_camera_dei_deputati['Fine'] = combined_table[('Mandato', 'Fine')]
presidenti_camera_dei_deputati[['Nome', 'Cognome']] = presidenti_camera_dei_deputati['Presidenti Camera'].str.split(n=1, expand=True)
presidenti_camera_dei_deputati = presidenti_camera_dei_deputati.drop(columns=['Presidenti Camera'])
# Stampa il DataFrame finale
#print(presidenti_camera_dei_deputati)

import pandas as pd

# ... (definizione dei dataframe df_finale e presidenti_camera_dei_deputati) ...

# Funzione per normalizzare il nome nel formato "Nome Cognome"
def normalize_name(name):
    name_parts = name.split()
    name_parts = [part.capitalize() for part in name_parts]  # Capitalizziamo ogni parola
    return " ".join(name_parts)

# Normalizziamo i nomi nei dataframe
df_totale['nome'] = df_totale['nome'].apply(normalize_name)
df_totale['cognome'] = df_totale['cognome'].apply(normalize_name)

presidenti_camera_dei_deputati['Nome'] = presidenti_camera_dei_deputati['Nome'].apply(normalize_name)
presidenti_camera_dei_deputati['Cognome'] = presidenti_camera_dei_deputati['Cognome'].apply(normalize_name)

# Lista per le corrispondenze trovate
corrispondenze = []

# Lista per i nomi che non hanno corrispondenze
nomi_non_trovati = []

# Effettuiamo il confronto tra i nomi nei due dataframe
for index, row in presidenti_camera_dei_deputati.iterrows():
    nome_presidente = row['Nome']
    cognome_presidente = row['Cognome']
    inizio = row['Inizio']
    fine = row['Fine']

    # Troviamo corrispondenze esatte nei dataframe
    matches = df_totale[(df_totale['nome'] == nome_presidente) & (df_totale['cognome'] == cognome_presidente)]

    if not matches.empty:
        # Se troviamo una corrispondenza, la aggiungiamo alla lista delle corrispondenze
        match = matches.iloc[0]  # Prendiamo solo la prima corrispondenza nel caso ce ne siano più di una
        corrispondenze.append({
            'Nome Presidente': nome_presidente,
            'Cognome Presidente': cognome_presidente,
            'Persona': match['persona'],
            'Gender': match['gender'],
            'Inizio': inizio,  
            'Fine': fine
        })
        df_totale = df_totale.drop(match.name)
    else:
        nomi_non_trovati.append({
            'Nome': nome_presidente,
            'Cognome': cognome_presidente,
            'Inizio': inizio,  # Inseriamo "Inizio" dal DataFrame presidenti_camera_dei_deputati
            'Fine': fine      # Inseriamo "Fine" dal DataFrame presidenti_camera_dei_deputati
        })


# Creiamo un DataFrame per le corrispondenze trovate
risultato_corrispondenze = pd.DataFrame(corrispondenze)

# Creiamo un DataFrame per le persone non trovate
persone_non_trovate = pd.DataFrame(nomi_non_trovati)
"""
# Stampa del DataFrame con le corrispondenze trovate
print("\nDataFrame con le corrispondenze trovate:")
print(risultato_corrispondenze)
"""
import pandas as pd
from fuzzywuzzy import fuzz

# DataFrame iniziali
# df_totale contiene le colonne 'persona', 'nome', 'cognome', e 'gender'
# persone_non_trovate contiene le colonne 'Nome' e 'Cognome'

# ... (inserisci qui i tuoi dataframe df_totale e persone_non_trovate)

# Funzione per normalizzare il nome nel formato "Nome Cognome"
def normalize_name(name):
    name_parts = name.split()
    name_parts = [part.capitalize() for part in name_parts]  # Capitalizziamo ogni parola
    return " ".join(name_parts)

# Normalizziamo i nomi nei dataframe
df_totale['nome'] = df_totale['nome'].apply(normalize_name)
df_totale['cognome'] = df_totale['cognome'].apply(normalize_name)
#("ciao")
#print(df_totale)
persone_non_trovate['Nome'] = persone_non_trovate['Nome'].apply(normalize_name)
persone_non_trovate['Cognome'] = persone_non_trovate['Cognome'].apply(normalize_name)


#print(persone_non_trovate)
# Funzione per normalizzare il nome nel formato "Nome Cognome"
def normalize_name(name):
    name_parts = name.split()
    name_parts = [part.capitalize() for part in name_parts]  # Capitalizziamo ogni parola
    return " ".join(name_parts)

# Normalizziamo i nomi nei dataframe
df_totale['nome'] = df_totale['nome'].apply(normalize_name)
df_totale['cognome'] = df_totale['cognome'].apply(normalize_name)
persone_non_trovate['Nome'] = persone_non_trovate['Nome'].apply(normalize_name)
persone_non_trovate['Cognome'] = persone_non_trovate['Cognome'].apply(normalize_name)

# Uniamo nome e cognome in un'unica colonna "nome_cognome" nei dataframe
df_totale['nome_cognome'] = df_totale['nome'] + " " + df_totale['cognome']
persone_non_trovate['nome_cognome'] = persone_non_trovate['Nome'] + " " + persone_non_trovate['Cognome']
print("personeeee")
print(persone_non_trovate)
# Lista per le corrispondenze trovate
corrispondenze = []

# Effettuiamo il confronto tra i nomi normalizzati nei due dataframe
for index, row in persone_non_trovate.iterrows():
    nome_cognome = row['nome_cognome']
    
    # Troviamo il miglior match per il nome_cognome nel DataFrame df_totale
    best_match = None
    best_name_ratio = 0
    
    for _, df_row in df_totale.iterrows():
        df_nome_cognome = df_row['nome_cognome']
        name_ratio = fuzz.token_set_ratio(nome_cognome, df_nome_cognome)

        if name_ratio > best_name_ratio:
            best_name_ratio = name_ratio
            best_match = df_row

    if best_match is not None and best_name_ratio >= 80:
        # Abbiamo trovato una corrispondenza per il nome, quindi aggiungiamo i valori corrispondenti al DataFrame delle corrispondenze
        corrispondenze.append({
            'Nome': row['Nome'],
            'Cognome': row['Cognome'],
            'Gender': best_match['gender'],
            'Persona': best_match['persona'],
            'Nome Presidente': best_match['nome'],
            'Cognome Presidente': best_match['cognome'],
            'Inizio': row['Inizio'],
            'Fine': row['Fine']
        })

# Creiamo un DataFrame per le corrispondenze trovate
risultato_corrispondenze2 = pd.DataFrame(corrispondenze)

# Stampa del DataFrame con le corrispondenze trovate
print("\nRisultato corrispondenze 2:")
print(risultato_corrispondenze2)


# Rimuoviamo eventuali corrispondenze duplicate
#risultato_corrispondenze2.drop_duplicates(subset=['nome', 'cognome'], keep='first', inplace=True)
#risultato_corrispondenze2 = risultato_corrispondenze2.rename(columns={'Nome': 'Nome Presidente'})
risultato_corrispondenze2 = risultato_corrispondenze2.rename(columns={'gender': 'Gender'})
risultato_corrispondenze2 = risultato_corrispondenze2.rename(columns={'persona': 'Persona'})
#risultato_corrispondenze2 = risultato_corrispondenze2.rename(columns={'Cognome': 'Cognome Presidente'})
risultato_corrispondenze2.drop(columns=['Nome', 'Cognome'], inplace=True)
# Stampa del DataFrame con le corrispondenze trovate

print("\nDataFrame2 con le corrispondenze trovate:")
print(risultato_corrispondenze2)
print("\nDataFrame con le corrispondenze trovate:")
print(risultato_corrispondenze)

presidenti_camera_dei_deputati_df = pd.concat([risultato_corrispondenze2, risultato_corrispondenze], ignore_index=True)
print(presidenti_camera_dei_deputati_df)
colonne = ['Nome Presidente', 'Cognome Presidente', 'Inizio', 'Fine', 'Gender']
presidenti_camera_dei_deputati_df[colonne].to_csv("presidenticamera.csv", index=False)

