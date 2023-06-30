import requests
import pandas as pd
from bs4 import BeautifulSoup
import sparql_dataframe
from sparql_dataframe import get

pd.set_option('display.max_rows', None)
endpoint = "https://dati.camera.it/sparql"


queryministri = """SELECT DISTINCT ?legislaturaLabel ?governoLabel ?membroLabel ?nome ?cognome 
 
WHERE { ?legislatura rdf:type ocd:legislatura;
                      rdfs:label ?legislaturaLabel;
                     ocd:rif_governo ?governo.
              ?governo rdfs:label ?governoLabel;
                     ocd:rif_membroGoverno  ?membro.
       ?membro rdfs:label ?membroLabel;
              foaf:firstName ?nome;
            foaf:surname ?cognome . 
        FILTER(contains(lcase(str(?membroLabel)), "ministro"))
} """

df_ministri_legislature = get(endpoint, queryministri)

df_ministri_legislature["legislaturaLabel"] = df_ministri_legislature["legislaturaLabel"].str.split(" ", n=1).str[0]
#df_ministri_legislature["governoLabel"] = df_ministri_legislature["governoLabel"].str.split(" ", n=1).str[0]
df_governi = df_ministri_legislature[['governoLabel']].copy()
df_governi['Governo'] = df_governi['governoLabel'].str.extract(r'^(.*?)\s*\(')
df_governi['data inizio'] =df_governi['governoLabel'].str.extract(r'\((.*?)\s*-\s*')
df_governi['data fine'] = df_governi['governoLabel'].str.extract(r'\-\s*(.*?)\)$')

# Rimuovi eventuali spazi iniziali e finali
df_governi['Governo'] = df_governi['Governo'].str.strip()
df_governi['data inizio'] = df_governi['data inizio'].str.strip()
df_governi['data fine'] = df_governi['data fine'].str.strip()

# Rimuovi la colonna originale se non più necessaria
df_governi = df_governi.drop('governoLabel', axis=1)
df_governi = df_governi.drop_duplicates()
lista_governi = df_governi["Governo"].to_list()
print(lista_governi)

import re

nuova_lista = []

for governo in lista_governi:
    parti = governo.split()
    nuovo_nome = "Governo_" + "_".join(parti[2:]) + "_" + parti[0]
    nuova_lista.append(nuovo_nome)

print(nuova_lista)


url_governi = []


url_governi = []

base_url = "https://it.wikipedia.org/wiki/"

for governo in nuova_lista:
    formatted_governo = governo
    url = base_url + formatted_governo

    response = requests.get(url)
    if response.status_code == 200:
        url_governi.append(url)
    else:
        url_governi.append(None)

print(url_governi)

#print(len(df_governi)) #68 precisi precisi 
#print(df_governi)
import requests
from bs4 import BeautifulSoup


# Stampa il dataframe

import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.governo.it/it/i-governi-dal-1943-ad-oggi/governo-meloni/20727"

response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Trova tutti gli elementi "a" nel documento HTML
links = soup.find_all('a')

# Estrai gli attributi "href" dagli elementi "a"
hrefs = []
for link in links:
    href = link.get('href')
    if href and (href.startswith('http://') or href.startswith('https://')):
        hrefs.append(href)

# Crea il dataframe con i risultati
df = pd.DataFrame(hrefs, columns=['URL'])

# Stampa il dataframe
df_filtered = df[df['URL'].str.contains('www.governo.it/it/i-governi-dal-1943-ad-oggi')]
lista_url = df_filtered["URL"].to_list()

print(df_filtered)
print(len(df_filtered))


# Liste per i dati dei governi
governi = []
coalizioni = []

for url in lista_url:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    info_block = soup.find('div', class_='field-items')
    
    nome_governo = ''
    if info_block.find('p'):
        nome_governo = info_block.find('p').text.strip().split('\n')[0]
    governi.append(nome_governo)

    blockquote = info_block.find('blockquote')
    coalizione = ''
    if blockquote:
        text_parts = blockquote.text.strip().split('\n')
        if len(text_parts) > 1:
            coalizione_parts = text_parts[1].split(':')
            if len(coalizione_parts) > 1:
                coalizione = coalizione_parts[1].strip()

    coalizioni.append(coalizione)

data = {'Governo': governi, 'Coalizione politica': coalizioni}
df_ = pd.DataFrame(data)






