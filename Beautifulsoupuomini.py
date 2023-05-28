from bs4 import BeautifulSoup
import requests 
from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get
import requests
from bs4 import BeautifulSoup
import re
import os 

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
#print(len(df_senza_url))


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

nomi_da_cercare = df_senza_url["Persona"].tolist()
# DataFrame vuoto per i risultati
df_risultati = pd.DataFrame(columns=["Persona", "URL"])

for nome_cognome in nomi_da_cercare:
    # Filtra il dataframe per le righe che contengono il nome e cognome nell'URL
    df_filtered = df_con_url2[df_con_url2["URL"].str.contains(nome_cognome.replace(" ", "_"))]

    # Se ci sono righe corrispondenti, prendi la prima riga
    if len(df_filtered) > 0:
        prima_riga = df_filtered.iloc[0]
        df_risultati = df_risultati.append(prima_riga)

#print("Risultati:")
#print(df_risultati)

dffinale = pd.concat([df_con_url, df_risultati], axis=0)

# Resetta l'indice del DataFrame risultante
dffinale = dffinale.reset_index(drop=True)
#print(dffinale)
#print(len(dffinale)) #134 hanno la pagina wikipedia mentre 28 no quindi non hanno info sul livello di educazione 

# TROVARE SE NEL DFFINALE NELLA SEZIONE BIOGRAFIA C'è LA PAROLA LAUREA ECC. 
#url_lista_finale = dffinale["URL"].tolist()
"""
df_con_parola = pd.DataFrame(columns=["Persona", "URL"])
df_senza_parola = pd.DataFrame(columns=["Persona", "URL"])

for url in url_lista_finale:
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        h2_elements = soup.find_all("h2")

        found = False  # Flag per indicare se è stata trovata la parola "laurea", "laureò" o "laureata"

        for h2 in h2_elements:
            sibling_p = h2.find_next_sibling("p")
            while sibling_p:
                if re.search(r"\b(laurea|laureò|laureata)\b", sibling_p.get_text(), re.IGNORECASE):
                    persona = os.path.basename(url)
                    df_con_parola = pd.concat([df_con_parola, pd.DataFrame({"Persona": [persona], "URL": [url]})], ignore_index=True)
                    found = True
                    break
                sibling_p = sibling_p.find_next_sibling("p")
            if found:
                break

        if not found:
            persona = os.path.basename(url)
            df_senza_parola = pd.concat([df_senza_parola, pd.DataFrame({"Persona": [persona], "URL": [url]})], ignore_index=True)

    else:
        print(f"Errore nella richiesta della pagina di Wikipedia per l'URL: {url}")

# Stampa dei DataFrame
print("Pagine con almeno una delle parole:")
print(df_con_parola)
print(len(df_con_parola))

print("Pagine senza nessuna delle parole:")
print(df_senza_parola)
print(len(df_senza_parola))
"""
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

url_lista_finale = dffinale["URL"].tolist()
"""
# Lista degli URL di Wikipedia
df_con_laurea = pd.DataFrame(columns=["Persona", "URL"])
df_con_diploma = pd.DataFrame(columns=["Persona", "URL"])
df_senza_sezione = pd.DataFrame(columns=["Persona", "URL"])

for url in url_lista_finale:
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        # Cerca la sezione "infobox sinottico"
        infobox = soup.find("table", class_="infobox sinottico")
        if infobox:
            dati_general_tr = infobox.find("tr", class_="sinottico_divisione")
            if dati_general_tr:
                dati_general_th = dati_general_tr.find("th", colspan="2", style="background:lavender;")
                if dati_general_th and dati_general_th.text.strip() == "Dati generali":
                    titolo_studio_th = infobox.find("th", string="Titolo\u00a0di\u00a0studio")
                    if titolo_studio_th:
                        titolo_studio_td = titolo_studio_th.find_next_sibling("td")
                        if titolo_studio_td:
                            titolo_studio_text = titolo_studio_td.get_text().lower()
                            if re.search(r"\blaurea\b", titolo_studio_text) or re.search(r"\bLaurea\b", titolo_studio_text):
                                persona = infobox.find("th").get_text()
                                df_con_laurea = df_con_laurea.append({"Persona": persona, "URL": url}, ignore_index=True)
                            elif re.search(r"\bdiploma\b", titolo_studio_text) or re.search(r"\bDiploma\b", titolo_studio_text):
                                persona = infobox.find("th").get_text()
                                df_con_diploma = df_con_diploma.append({"Persona": persona, "URL": url}, ignore_index=True)
                    else:
                        persona = infobox.find("th").get_text()
                        df_senza_sezione = df_senza_sezione.append({"Persona": persona, "URL": url}, ignore_index=True)
                else:
                    persona = infobox.find("th").get_text()
                    df_senza_sezione = df_senza_sezione.append({"Persona": persona, "URL": url}, ignore_index=True)
            else:
                persona = infobox.find("th").get_text()
                df_senza_sezione = df_senza_sezione.append({"Persona": persona, "URL": url}, ignore_index=True)

print("Persone con laurea:")
print(df_con_laurea)

print("Persone con diploma:")
print(df_con_diploma)

print("Persone senza sezione 'Titolo di studio':")
print(df_senza_sezione)

"""
import requests
from bs4 import BeautifulSoup
import pandas as pd

import requests
from bs4 import BeautifulSoup
import pandas as pd

def check_titolo_studio(soup):
    infobox = soup.find('table', {'class': 'infobox sinottico'})
    if infobox:
        titolo_studio = infobox.find("th", text="Titolo di studio")
        if titolo_studio:
            sezione_titolo_studio = titolo_studio.find_next('td')
            if sezione_titolo_studio and ('laurea' in sezione_titolo_studio.text.lower() or 'diploma' in sezione_titolo_studio.text.lower()):
                return True
    return False

url_list = [
    'https://it.wikipedia.org/wiki/Ludovico_Boetti_Villanis_Audifredi',
    # Aggiungi qui gli altri URL
]

df_laurea = pd.DataFrame(columns=['Persona', 'URL'])
df_diploma = pd.DataFrame(columns=['Persona', 'URL'])
df_senza_sezione = pd.DataFrame(columns=['Persona', 'URL'])

for url in url_list:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    persona = soup.find('title').text.split('-')[0].strip()

    if check_titolo_studio(soup):
        sezione_titolo_studio = soup.find('table', {'class': 'infobox sinottico'}).find("th", text="Titolo di studio").find_next('td').text.strip().lower()
        if 'laurea' in sezione_titolo_studio:
            df_laurea = df_laurea.append({"Persona": persona, "URL": url}, ignore_index=True)
        if 'diploma' in sezione_titolo_studio:
            df_diploma = df_diploma.append({"Persona": persona, "URL": url}, ignore_index=True)
    else:
        df_senza_sezione = df_senza_sezione.append({"Persona": persona, "URL": url}, ignore_index=True)

print("URL con la parola 'laurea':")
print(df_laurea)

print("URL con la parola 'diploma':")
print(df_diploma)

print("URL senza la sezione 'Titolo di studio':")
print(df_senza_sezione)
