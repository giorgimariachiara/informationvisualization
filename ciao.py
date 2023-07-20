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
df_totale_donne = df_totale_donne[['persona','nome', 'cognome', 'gender']]

#QUERY NUMERO TOTALE UOMINI
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

df_totale = pd.concat([df_totale_uomini, df_totale_donne])
#df_totale.to_csv("totaledeputati.csv",  index=False, index_label=False)
#print(df_totale)

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
print(third_table[('Presidente', 'Presidente.2')])

# Mostra la quarta tabella
print("Quarta tabella:")
#print(fourth_table[('Presidente', 'Presidente.2')])

combined_column = pd.concat([third_table[('Presidente', 'Presidente.2')], fourth_table[('Presidente', 'Presidente.2')]])

# Estrai solo i nomi e cognomi senza date utilizzando espressioni regolari
pattern = r"([A-Za-z\s-]+)"
nomi = combined_column.str.extract(pattern, expand=False)
nomi = nomi.drop_duplicates()
presidenti_camera_dei_deputati = nomi.rename("Presidenti Camera")

# Converte la Serie in un DataFrame
presidenti_camera_dei_deputati = pd.DataFrame(presidenti_camera_dei_deputati)

# Verifica se la colonna 'Presidenti Camera' è presente nel DataFrame
if 'Presidenti Camera' in presidenti_camera_dei_deputati.columns:
    # Dividi i nomi in due colonne separate per nome e cognome
    presidenti_camera_dei_deputati[['Nome', 'Cognome']] = presidenti_camera_dei_deputati['Presidenti Camera'].str.split(n=1, expand=True)
    
    # Rimuovi la colonna 'Presidenti Camera' non necessaria
    presidenti_camera_dei_deputati = presidenti_camera_dei_deputati.drop(columns='Presidenti Camera')

# Stampa i nomi e cognomi
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

    # Troviamo corrispondenze esatte nei dataframe
    matches = df_totale[(df_totale['nome'] == nome_presidente) & (df_totale['cognome'] == cognome_presidente)]

    if not matches.empty:
        # Se troviamo una corrispondenza, la aggiungiamo alla lista delle corrispondenze
        match = matches.iloc[0]  # Prendiamo solo la prima corrispondenza nel caso ce ne siano più di una
        corrispondenze.append({
            'Nome Presidente': nome_presidente,
            'Cognome Presidente': cognome_presidente,
            'Persona': match['persona'],
            'Gender': match['gender']
        })
        df_totale = df_totale.drop(match.name)
    else:
        nomi_non_trovati.append((nome_presidente, cognome_presidente))

# Creiamo un DataFrame per le corrispondenze trovate
risultato_corrispondenze = pd.DataFrame(corrispondenze)

# Creiamo un DataFrame per le persone non trovate
persone_non_trovate = pd.DataFrame(nomi_non_trovati, columns=['Nome', 'Cognome'])

# Stampa del DataFrame con le corrispondenze trovate
print("\nDataFrame con le corrispondenze trovate:")
print(risultato_corrispondenze)

print(df_totale.columns)
print(persone_non_trovate.columns)

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

persone_non_trovate['Nome'] = persone_non_trovate['Nome'].apply(normalize_name)
persone_non_trovate['Cognome'] = persone_non_trovate['Cognome'].apply(normalize_name)

# Lista per le corrispondenze trovate
corrispondenze = []

# Funzione per trovare la migliore corrispondenza tra i nomi
def find_matching_person(nome_cognome):
    best_match = None
    best_ratio = 0

    for _, row in presidenti_camera_dei_deputati.iterrows():
        cognome = row['Cognome']
        nome = row['Nome']
        full_name = nome + " " + cognome
        ratio = fuzz.token_sort_ratio(nome_cognome, full_name)

        if ratio > best_ratio:
            best_ratio = ratio
            best_match = (nome, cognome)

    return best_match if best_ratio >= 70 else None

# Effettuiamo il confronto tra i nomi normalizzati nel dataframe persone_non_trovate
for index, row in persone_non_trovate.iterrows():
    nome_cognome = row['Nome'] + " " + row['Cognome']
    match = find_matching_person(nome_cognome)

    if match:
        # Se abbiamo trovato una corrispondenza, aggiungiamola al DataFrame delle corrispondenze
        corrispondenze.append({
            'nome': row['Nome'],
            'cognome': row['Cognome'],
            'gender_df_totale': df_totale.loc[index, 'gender'],  # Aggiungiamo il gender da df_totale
            'persona_df_totale': df_totale.loc[index, 'persona'],  # Aggiungiamo la persona da df_totale
            'Nome': match[0],
            'Cognome': match[1]
        })

# Creiamo un DataFrame per le corrispondenze trovate
risultato_corrispondenze = pd.DataFrame(corrispondenze)

# Rimuoviamo eventuali corrispondenze duplicate
risultato_corrispondenze.drop_duplicates(subset=['nome', 'cognome'], keep='first', inplace=True)

# Stampa del DataFrame con le corrispondenze trovate
print("\nDataFrame con le corrispondenze trovate:")
print(risultato_corrispondenze)





