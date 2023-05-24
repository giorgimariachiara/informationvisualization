from bs4 import BeautifulSoup
import requests 
from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get
import requests
from bs4 import BeautifulSoup
import re

endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)


import urllib.parse
pd.set_option("display.max_colwidth", None)


queryprovaa ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
?dataNascita ?luogoNascita 
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
dataprova = get(endpoint, queryprovaa)
dataprova = dataprova.drop_duplicates(["persona","nome", "cognome", "luogoNascita"])
df_nana = dataprova[dataprova['info'].isna()] 
dataprova = dataprova[["nome","cognome","info", "luogoNascita"]]
df_nana = dataprova[dataprova['info'].isnull()] #qui le donne senza info diventano solo 49 
df_nana = df_nana[["nome", "cognome"]]
nomi = df_nana['nome'] + ' ' + df_nana['cognome']
nomiuomini = nomi.to_list()
listauomini= [nome_cognome.title() for nome_cognome in nomiuomini]
listauomini = [nome_cognome.replace(' ', '_') for nome_cognome in listauomini]
#print(len(listauomini))
#print(nomiuomini)
#print(len(nomiuomini))
"""

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
da = get_uri_from_names(nomiuomini)
da = da.drop_duplicates(["persona", "nome", "cognome"])
da = da[["nome", "cognome", "uri"]]
"""

url_lista = []
listauomini_con_url = []
listauomini_senza_url = []

for persona in listauomini:
    url = f"https://it.wikipedia.org/wiki/{persona}"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"URL della pagina di Wikipedia per {persona}: {url}")
        url_lista.append(url)
        listauomini_con_url.append(persona)
    else:
        print(f"Errore nella richiesta della pagina di Wikipedia per {persona}")
        listauomini_senza_url.append(persona)

df_con_url = pd.DataFrame({"Persona": listauomini_con_url, "URL": url_lista})
df_senza_url = pd.DataFrame({"Persona": listauomini_senza_url})

#print("Persone con URL:")
#print(df_con_url)
#print(len(df_con_url))

#print("Persone senza URL:")
#print(df_senza_url)
print(len(df_senza_url))


url_lista = []
personemodificato_con_url = []

for persona in df_senza_url["Persona"]:
    nome_cognome_parts = persona.split("_")  # Dividi la stringa utilizzando l'underscore come separatore
    nome = nome_cognome_parts[0]  # Primo nome
    cognome = nome_cognome_parts[-1]  # Ultimo cognome

    # Effettua la richiesta HTTP alla pagina di Wikipedia
    response = requests.get(f"https://it.wikipedia.org/wiki/{persona}")

    if response.status_code == 200:
        url_lista.append(response.url)
        personemodificato_con_url.append(persona)
    else:
        # Cerca gli URL alternativi che contengono nome e cognome in diverse combinazioni
        search_query = f"https://it.wikipedia.org/w/index.php?title=Speciale:Search&search={nome}+{cognome}"
        search_response = requests.get(search_query)

        if search_response.status_code == 200:
            search_soup = BeautifulSoup(search_response.content, "html.parser")
            search_results = search_soup.find_all("div", class_="mw-search-result-heading")

            for result in search_results:
                result_link = result.find("a")
                result_url = result_link["href"]
                url = f"https://it.wikipedia.org{result_url}"
                decoded_url = urllib.parse.unquote(url)  # Decodifica l'URL per visualizzare i caratteri speciali
                url_lista.append(decoded_url)
                personemodificato_con_url.append(persona)
        else:
            print(f"Errore nella richiesta della pagina di Wikipedia per {persona}")

df_con_url2 = pd.DataFrame({"Persona": personemodificato_con_url, "URL": url_lista})

print("Persone con URL:")
print(df_con_url2)
print(len(df_con_url2))

nomi_da_cercare = df_senza_url["Persona"].tolist()
# DataFrame vuoto per i risultati
df_risultati = pd.DataFrame(columns=["Persona", "URL"])

for nome_cognome in nomi_da_cercare:
    # Filtra il dataframe per le righe che contengono il nome e cognome nell'URL
    df_filtered = df_con_url[df_con_url["URL"].str.contains(nome_cognome.replace(" ", "_"))]

    # Se ci sono righe corrispondenti, prendi la prima riga
    if len(df_filtered) > 0:
        prima_riga = df_filtered.iloc[0]
        df_risultati = df_risultati.append(prima_riga)

print("Risultati:")
print(df_risultati)