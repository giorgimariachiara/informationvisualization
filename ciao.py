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
print(df_totale)

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
print(presidenti_camera_dei_deputati)


risultato = pd.DataFrame(columns=['nome', 'cognome', 'gender', 'Persona'])

for index, row in df_totale.iterrows():
    nome = row['nome']
    cognome = row['cognome']
    nome_check = nome.lower()
    cognome_check = cognome.lower().replace('-', '')  # Conversione in minuscolo e rimozione del trattino per il confronto
    
    # Controllo della corrispondenza nel DataFrame presidenti_camera_dei_deputati
    match = presidenti_camera_dei_deputati[
        (presidenti_camera_dei_deputati['Cognome'].str.lower().replace('-', '') == cognome_check) &
        (presidenti_camera_dei_deputati['Nome'].str.lower().str.contains(nome_check))
    ]
    
    if match.empty:
        # Ricerca per il cognome nel DataFrame presidenti_camera_dei_deputati
        match_cognome = presidenti_camera_dei_deputati[
            presidenti_camera_dei_deputati['Cognome'].str.lower().replace('-', '') == cognome_check
        ]
        
        # Controllo se il cognome è stato trovato e se il nome è contenuto nella stringa
        if not match_cognome.empty and nome_check in match_cognome['Nome'].str.lower().tolist():
            match = match_cognome[match_cognome['Nome'].str.lower().str.contains(nome_check)]
    
    if not match.empty:
        # Aggiunta della riga corrispondente al DataFrame risultante
        risultato = risultato.append({
            'nome': nome,
            'cognome': cognome,
            'gender': row['gender'],
            'Persona': row['persona']
        }, ignore_index=True)

# Stampa del DataFrame risultante
print(risultato)

#devo finire 