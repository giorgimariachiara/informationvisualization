from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get
import requests
from bs4 import BeautifulSoup
import re
import os 
import urllib.parse


endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)

querydefinitivalaureauomini ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
?dataNascita ?luogoNascita 
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

df_laurea_donne = get(endpoint, querydefinitivalaureauomini)
df_laurea_donne = df_laurea_donne.drop_duplicates(["persona","nome", "cognome", "luogoNascita"]) #5204
df_laurea_donne_data = df_laurea_donne.drop_duplicates(["persona","nome", "cognome", "dataNascita","luogoNascita"])
df_donne_noinfo = df_laurea_donne[df_laurea_donne['info'].isna()] 
df_donne_noinfo_data = df_donne_noinfo[["nome", "cognome", "dataNascita"]]

import locale
from datetime import datetime

#Modifica della formattazione dei nomi per permettere la ricerca degli URL wikipedia 
locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')

lista_politici_data = []

def capitalize_name(name):
    parts = re.split(r"([ '-])", name)
    return "".join([part.capitalize() for part in parts])

for index, row in df_donne_noinfo_data.iterrows():
    nome = row['nome'].split()
    cognome = row['cognome'].split()
    nome_cognome = "_".join([capitalize_name(word) for word in nome + cognome])
    data_nascita = row['dataNascita']
    if pd.notna(data_nascita):
        data = datetime.strptime(str(int(data_nascita)), "%Y%m%d")
        giorno = str(data.day)  # Converte il giorno in stringa
        mese = data.strftime("%B")  # Ottiene il nome completo del mese
        anno = str(data.year)  # Converte l'anno in stringa
        data_formattata = f"{giorno} {mese} {anno}"
        lista_politici_data.append((nome_cognome, data_formattata))
    else:
        lista_politici_data.append((nome_cognome, ""))



df_laurea_donne['info'] = df_laurea_donne['info'].fillna('') 
masklaurea = df_laurea_donne['info'].str.contains('Laurea|laurea|Master|LAUREA')

# Estrazione delle persone che hanno laurea dai dati di dati camera 
laureate = df_laurea_donne[masklaurea]
laureate = laureate.assign(gender='female')
donnelaureate = laureate[["nome", "cognome", "gender"]]

masknonlaurea =~df_laurea_donne['info'].str.contains('Laurea|laurea|Master|LAUREA', na=False) & df_laurea_donne['info'].ne('')
donnenonlaureate = df_laurea_donne[masknonlaurea]
#uomininonlaureati = uomininonlaureati.assign(info="no")
donnenonlaureate = donnenonlaureate.assign(gender='female')
donnenonlaureate = donnenonlaureate[["nome", "cognome", "gender"]]

print("DONNE TOTALE 905")
print(len(df_laurea_donne)) 
print("DONNE LAUREATE 569")
print(len(donnelaureate)) 
print("DONNE NO INFO 49")
print(len(df_donne_noinfo)) 
print("DONNE NON LAUREATE 287")
print(len(donnenonlaureate))

#CERCA PAGINA WIKIPEDIA DELLE PERSONE SENZA INFORMAZIONI SUL TITOLO DI STUDIO 

import re
from bs4 import BeautifulSoup

def check_url_exists(url):
    response = requests.get(url)
    return response.status_code == 200

def extract_birth_date(text):
    if '–' in text:
        date_range = re.findall(r'(\d+\s\w+\s\d+)', text)
        if date_range:
            birth_date = date_range[0]
            if birth_date in text.split('–')[0]:
                return birth_date
    else:
        matches = re.findall(r'(\d+\s\w+\s\d+)', text)
        if matches:
            return matches[0]
    return None

url_lista = []
donne_con_url = []
donne_senza_url = []

for nome_cognome, data_nascita in lista_politici_data:
    url_politico_data = None
    for anno_iniziale in [1952, 1940, 1929]:
        url_politico_data = f"https://it.wikipedia.org/wiki/{nome_cognome}_(politico_{anno_iniziale})"
        if check_url_exists(url_politico_data):
            pagina_wikipedia = requests.get(url_politico_data)
            if pagina_wikipedia.status_code == 200:
                soup = BeautifulSoup(pagina_wikipedia.text, 'html.parser')
                paragraph = soup.find('p')
                if paragraph:
                    birth_date = extract_birth_date(paragraph.text)
                    if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                        #print(f"URL della pagina di Wikipedia per {nome_cognome} (politico con data): {url_politico_data}")
                        url_lista.append(url_politico_data)
                        donne_con_url.append(nome_cognome)
                        break
                else:
                    print("Paragrafo non trovato.")
    else:
        url_politico = f"https://it.wikipedia.org/wiki/{nome_cognome}_(politico)"
        url_generale = f"https://it.wikipedia.org/wiki/{nome_cognome}"
        if check_url_exists(url_politico):
            pagina_wikipedia = requests.get(url_politico)
            if pagina_wikipedia.status_code == 200:
                soup = BeautifulSoup(pagina_wikipedia.text, 'html.parser')
                paragraph = soup.find('p')
                if paragraph:
                    birth_date = extract_birth_date(paragraph.text)
                    if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                        #print(f"URL della pagina di Wikipedia per {nome_cognome} (politico): {url_politico}")
                        url_lista.append(url_politico)
                        donne_con_url.append(nome_cognome)
                        continue
                else:
                    print("Paragrafo non trovato.")
        if check_url_exists(url_generale):
            pagina_wikipedia = requests.get(url_generale)
            if pagina_wikipedia.status_code == 200:
                soup = BeautifulSoup(pagina_wikipedia.text, 'html.parser')
                paragraph = soup.find('p')
                if paragraph:
                    birth_date = extract_birth_date(paragraph.text)
                    if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                        #print(f"URL della pagina di Wikipedia generica per {nome_cognome}: {url_generale}")
                        url_lista.append(url_generale)
                        donne_con_url.append(nome_cognome)
                        continue
                else:
                    print("Paragrafo non trovato.")
        print(f"Nessun URL trovato per {nome_cognome}")
        
        donne_senza_url.append((nome_cognome, data_nascita))
        

df_donne_con_url = pd.DataFrame({"Persona": donne_con_url, "URL": url_lista})
for index, row in df_donne_con_url.iterrows():
    persona = row["Persona"]
    for nome_cognome, data_nascita in lista_politici_data:
        if nome_cognome == persona:
            df_donne_con_url.at[index, "Data di nascita"] = data_nascita
            break

df_donne_senza_url = pd.DataFrame({"Persona e Data di nascita": donne_senza_url})
pd.set_option('display.max_colwidth', None)
print("Donne che hanno url subito 30")
print(len(df_donne_con_url))
print("Donne senza url subito 19")
print(len(df_donne_senza_url))

lista_donne_senza_url = df_donne_senza_url['Persona e Data di nascita'].values.tolist()

import pandas as pd
import wikipediaapi
import re

#Stesso controllo ma per vedere le date primo e per quelli che sono morti e quindi hanno due date su cui fare il check ecc 

def extract_birth_date(text):
    if '–' in text:
        date_range = re.findall(r'(\d+\s\w+\s\d+)', text)
        if date_range:
            birth_date = date_range[0]
            if birth_date in text.split('–')[0]:
                return birth_date
    else:
        matches = re.findall(r'(\d+º?\s\w+\s\d+)', text)
        if matches:
            birth_date = matches[0]
            if birth_date.startswith('1º'):
                return birth_date.replace('º', '')
            return birth_date

def find_wikipedia_url(nome_cognome, data_nascita):
    wiki_wiki = wikipediaapi.Wikipedia('it')

    url_politico_data = None
    url_politico = None
    url_generale = None

    for anno_iniziale in [1952, 1940, 1929]:
        page_name = f"{nome_cognome} (politico {anno_iniziale})"
        page = wiki_wiki.page(page_name)
        if page.exists():
            content = page.text
            birth_date = extract_birth_date(content)
            if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                url_politico_data = page.fullurl
                break

    if not url_politico_data:
        page_name = f"{nome_cognome} (politico)"
        page = wiki_wiki.page(page_name)
        if page.exists():
            content = page.text
            birth_date = extract_birth_date(content)
            if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                url_politico = page.fullurl

    if not url_politico_data and not url_politico:
        page_name = nome_cognome
        page = wiki_wiki.page(page_name)
        if page.exists():
            content = page.text
            birth_date = extract_birth_date(content)
            if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                url_generale = page.fullurl

    if url_politico_data:
        return (nome_cognome, data_nascita, url_politico_data)
    elif url_politico:
        return (nome_cognome, data_nascita, url_politico)
    elif url_generale:
        return (nome_cognome, data_nascita, url_generale)
    else:
        return (nome_cognome, data_nascita, None)

df_with_url = pd.DataFrame(columns=['Nome', 'Data di nascita', 'URL'])
df_without_url = pd.DataFrame(columns=['Nome', 'Data di nascita'])

for item in lista_donne_senza_url:
    nome_cognome, data_nascita, url = find_wikipedia_url(item[0], item[1])
    if url:
        df_with_url = pd.concat([df_with_url, pd.DataFrame({'Nome': [nome_cognome], 'Data di nascita': [data_nascita], 'URL': [url]})], ignore_index=True)
    else:
        df_without_url = pd.concat([df_without_url, pd.DataFrame({'Nome': [nome_cognome], 'Data di nascita': [data_nascita]})], ignore_index=True)

pd.set_option('display.max_colwidth', None)
print("DONNE CON URL DOPO IL PRIMO TENT 5:")
#print(df_with_url)
print(len(df_with_url))
print("DONNE SENZA URL ANCHE DOPO QUESTO TENT 14:")
print(len(df_without_url))


#Trovare quelli che hanno dei secondi nomi su wiki ecc

# Funzione per controllare l'esistenza dell'URL
def check_url_exists(url):
    response = requests.get(url)
    return response.status_code == 200

# Funzione per ottenere URL alternativi
def get_alternative_urls(nome, cognome):
    search_query = f"https://it.wikipedia.org/w/index.php?title=Speciale:Search&search={nome}+{cognome}"
    search_response = requests.get(search_query)

    alternative_urls = []

    if search_response.status_code == 200:
        search_soup = BeautifulSoup(search_response.content, "html.parser")
        search_results = search_soup.find_all("div", class_="mw-search-result-heading")

        for result in search_results:
            result_link = result.find("a")
            result_url = result_link["href"]
            url = f"https://it.wikipedia.org{result_url}"
            decoded_url = urllib.parse.unquote(url)  # Decodifica l'URL per visualizzare i caratteri speciali
            alternative_urls.append(decoded_url)

    return alternative_urls

url_lista = []
donne_con_url2 = []
donne_senza_url2 = []
date_nascita_senza_url2 = []
date_nascita_con_url2 = []

for index, row in df_without_url.iterrows():
    persona = row['Nome']
    nome_cognome_parts = persona.split("_")
    nome = nome_cognome_parts[0]
    cognome = nome_cognome_parts[-1]

    response = requests.get(f"https://it.wikipedia.org/wiki/{persona}", timeout=10)

    if response.status_code == 200:
        url_lista.append(response.url)
        donne_con_url2.append(persona)
        date_nascita_con_url2.append(row['Data di nascita'])
    else:
        alternative_urls = get_alternative_urls(nome, cognome)

        for alternative_url in alternative_urls:
            if check_url_exists(alternative_url):
                url_lista.append(alternative_url)
                donne_con_url2.append(persona)
                date_nascita_con_url2.append(row['Data di nascita'])
                break
        else:
            donne_senza_url2.append(persona)
            date_nascita_senza_url2.append(row['Data di nascita'])

df_donne_con_url2 = pd.DataFrame({"Persona": donne_con_url2, "URL": url_lista, "Data di nascita": date_nascita_con_url2})
df_donne_senza_url2 = pd.DataFrame({"Persona": donne_senza_url2, "Data di nascita": date_nascita_senza_url2})

print("Lunghezza dataframe uomini con URL 14:")
print(len(df_donne_con_url2))
print("Lunghezza dataframe uomini senza URL 0:")
print(len(df_donne_senza_url2))

