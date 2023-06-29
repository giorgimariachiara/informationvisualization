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
#print(len(df_governi)) #68 precisi precisi 

import requests
from bs4 import BeautifulSoup

url_list = [
    "https://www.governo.it/it/i-governi-dal-1943-ad-oggi/xiii-legislatura-9-maggio-1996-9-marzo-2001/governo-prodi/2944"
    
    # Aggiungi altri URL se necessario
]

# Liste per i dati dei governi
governi = []
coalizioni = []

# Itera attraverso gli URL
for url in url_list:
    # Effettua la richiesta GET al sito web
    response = requests.get(url)

    # Parsing del contenuto HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Trova il blocco di informazioni del governo
    info_block = soup.find('div', class_='field-items')

    # Estrai il nome del governo
    nome_governo = info_block.find('p').text.strip().split('\n')[0]
    governi.append(nome_governo)

    # Estrai la coalizione politica
    coalizione = info_block.find('blockquote').text.strip().split('\n')[1].split(':')[1].strip()
    coalizioni.append(coalizione)

# Crea il dataframe
data = {'Governo': governi, 'Coalizione politica': coalizioni}
df = pd.DataFrame(data)

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
print(df)





