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
#print(len(df_nana))
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
#print(len(personemodificato))

import requests
from bs4 import BeautifulSoup
import re


url_lista = []
personemodificato_con_url = []
personemodificato_senza_url = []

for persona in personemodificato:
    url = f"https://it.wikipedia.org/wiki/{persona}"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"URL della pagina di Wikipedia per {persona}: {url}")
        url_lista.append(url)
        personemodificato_con_url.append(persona)
    else:
        print(f"Errore nella richiesta della pagina di Wikipedia per {persona}")
        personemodificato_senza_url.append(persona)

df_con_url = pd.DataFrame({"Persona": personemodificato_con_url, "URL": url_lista})
df_senza_url = pd.DataFrame({"Persona": personemodificato_senza_url})
#print(df_senza_url)
"""
print("Persone con URL:")
print(df_con_url)

print("Persone senza URL:")
print(df_senza_url)
"""

#provo a trovare quelli di cui non abbiamo trovato l'url con diverse combinazioni 
import urllib.parse
pd.set_option("display.max_colwidth", None)

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


nomi_da_cercare = ["Alessandra Cecchetto", "Gigliola Lo Cascio", "Natia Mammone", "Roberta Pinto", "Daniela Romani", "Marisa Bonfatti Paini", "Agata Lucia Alma Cappiello", "Luigia Cordati ", " Anna Lucia Lisa Pannarale", "Maria Galli", "Ida Matarazzo"]

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
"""
# Cerca la parola "laurea", "università", "laureò" nella sezione biografia per ciascun URL
for url in url_lista:
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        h2_elements = soup.find_all("h2")

        found = False  # Flag per indicare se è stata trovata la parola "laurea"

        for h2 in h2_elements:
            sibling_p = h2.find_next_sibling("p")
            while sibling_p:
                if re.search(r"\b(laurea|laureò|laureata)\b", sibling_p.get_text(), re.IGNORECASE):
                    print(f"La parola 'laurea' è presente nella biografia per l'URL: {url}")
                    found = True
                    break
                sibling_p = sibling_p.find_next_sibling("p")
            if found:
                break

        if not found:
            print(f"La parola 'laurea' non è presente nella biografia per l'URL: {url}")
    else:
        print(f"Errore nella richiesta della pagina di Wikipedia per l'URL: {url}")

"""
"""
df_con_parola = pd.DataFrame(columns=["Persona", "URL"])
df_senza_parola = pd.DataFrame(columns=["Persona", "URL"])

for url in url_lista:
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        h2_elements = soup.find_all("h2")

        found = False  # Flag per indicare se è stata trovata la parola "laurea", "laureò" o "laureata"

        for h2 in h2_elements:
            sibling_p = h2.find_next_sibling("p")
            while sibling_p:
                if re.search(r"\b(laurea|laureò|laureata|facoltà)\b", sibling_p.get_text(), re.IGNORECASE):
                    df_con_parola = df_con_parola.append({"Persona": persona, "URL": url}, ignore_index=True)
                    found = True
                    break
                sibling_p = sibling_p.find_next_sibling("p")
            if found:
                break

        if not found:
            df_senza_parola = df_senza_parola.append({"Persona": persona, "URL": url}, ignore_index=True)
    else:
        print(f"Errore nella richiesta della pagina di Wikipedia per l'URL: {url}")

# Stampa dei DataFrame
#print("Pagine con almeno una delle parole:")
#print(df_con_parola)

#print("Pagine senza nessuna delle parole:")
#print(df_senza_parola)
"""
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