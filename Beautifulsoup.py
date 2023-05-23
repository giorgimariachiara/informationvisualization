from bs4 import BeautifulSoup
import requests 
from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get

endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)
# Lista di nomi e cognomi delle persone

queryprovaa ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
 ?luogoNascita 
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
dataprova = get(endpoint, queryprovaa)
dataprova = dataprova.drop_duplicates(["persona","nome", "cognome", "luogoNascita"])
dataprova = dataprova[["nome","cognome","info", "luogoNascita"]]
df_nana = dataprova[dataprova['info'].isnull()] #qui le donne senza info diventano solo 49 
df_nana = df_nana[["nome", "cognome"]]
nomi = df_nana['nome'] + ' ' + df_nana['cognome']
nomidonne = nomi.to_list()

def get_uri_from_names(lista):
    # Inizializza l'oggetto SPARQLWrapper
    sparql = SPARQLWrapper("http://tuo_endpoint_sparql")  
    risultati = []
    for nome_cognome in lista:
        # Costruisci la query SPARQL
        query = '''
            SELECT DISTINCT ?persona ?nome ?cognome ?uri
            WHERE {{
                ?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
                ?persona foaf:firstName ?nome.
                ?persona foaf:surname ?cognome. 
                ?persona owl:sameAs ?uri.
                FILTER (CONCAT(?nome, " ", ?cognome) = "{0}")
            }}
        '''.format(nome_cognome)

        queryy = get(endpoint, query)
        risultati.append(queryy)

    # Unisci tutti i dataframe in un unico dataframe finale
    df_finale = pd.concat(risultati)
    return df_finale  
da = get_uri_from_names(nomidonne)
da = da.drop_duplicates(["persona", "nome", "cognome"])
da = da[["nome", "cognome"]]

merged = pd.merge(da, df_nana, how='outer', indicator=True)
filtered = merged[merged['_merge'] != 'both']
result = filtered.drop('_merge', axis=1)
persone = result[["nome", "cognome"]]
persone = persone.apply(lambda row: row['nome'] + ' ' + row['cognome'], axis=1).tolist() 
personemodificato= [nome_cognome.title() for nome_cognome in persone]
personemodificato = [nome_cognome.replace(' ', '_') for nome_cognome in personemodificato]
print(personemodificato)

for persona in personemodificato:
    # Costruisci l'URL della pagina di Wikipedia per la persona corrente
    url = f"https://it.wikipedia.org/wiki/{persona}"

    # Effettua la richiesta HTTP alla pagina di Wikipedia
    response = requests.get(url)
    if response.status_code == 200:
        # Parsa l'HTML della pagina con Beautiful Soup
        soup = BeautifulSoup(response.content, "html.parser")

        # Esegui l'elaborazione dei dati o l'estrazione delle informazioni necessarie
        # Ad esempio, puoi trovare il titolo della pagina e stamparlo
        titolo_pagina = soup.find("h1", class_="firstHeading").text
        print(f"Pagina di Wikipedia per {persona}: {titolo_pagina}")
    else:
        print(f"Errore nella richiesta della pagina di Wikipedia per {persona}")
"""
url = "https://it.wikipedia.org/wiki/Grazia_Sestini"
response = requests.get(url)
html_content = response.text

soup = BeautifulSoup(html_content, "html.parser")
biografia_header = soup.find(id="Biografia")
if biografia_header:
    # Navigate to the parent section and extract the information
    biografia_section = biografia_header.find_parent("div", class_="mw-parser-output")
    if biografia_section:
        # Extract the text or do further processing
        biografia_text = biografia_section.get_text(strip=True)
        print(biografia_text)
else:
    print("Biografia header not found.") 
"""