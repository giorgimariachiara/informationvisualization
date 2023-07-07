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
#print(lista_governi)

import re

nuova_lista = []

for governo in lista_governi:
    parti = governo.split()
    nuovo_nome = "Governo_" + "_".join(parti[2:]) + "_" + parti[0]
    nuova_lista.append(nuovo_nome)

#print(nuova_lista)
#print(len(nuova_lista))


url_governi = []

base_url = "https://it.wikipedia.org/wiki/"

for governo in nuova_lista:
    formatted_governo = governo.replace("_", " ")
    url = base_url + formatted_governo

    response = requests.get(url)
    if response.status_code == 200:
        url_governi.append(url)
    else:
        # Prova a cercare l'URL senza il numero romano
        formatted_governo_no_roman = " ".join(formatted_governo.split()[:-1])
        url_no_roman = base_url + formatted_governo_no_roman

        response_no_roman = requests.get(url_no_roman)
        if response_no_roman.status_code == 200:
            url_governi.append(url_no_roman)
        else:
            url_governi.append(None)

#print(url_governi)
"""
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




"""


#print(urldaesaminare)
# Inizializza una lista per i dataframe
dataframes = []

# Itera sugli URL
for url in url_governi:
    response = requests.get(url)
    html_content = response.text

    # Analizzare l'HTML con BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Trovare tutti gli elementi <tr>
    tr_elements = soup.find_all("tr")

    # Inizializza le liste per i valori di <th> e <td>
    th_values = []
    td_values = []

    # Scorrere gli elementi <tr> e controllare i valori di <th> e <td>
    for tr_element in tr_elements:
        th_elements = tr_element.find_all("th")
        td_elements = tr_element.find_all("td")

        for th_element in th_elements:
            th_value = th_element.text.strip()
            th_values.append(th_value)

            # Se esiste un elemento <td> associato, aggiungi il suo valore alla lista
            if td_elements:
                td_value = td_elements[0].text.strip()
                td_values.append(td_value)
            else:
                td_values.append("")

    # Crea il dataframe utilizzando le liste di valori
    data = {"th": th_values, "td": td_values, "url": [url] * len(th_values)}
    df = pd.DataFrame(data)
    df_filt = df[df['th'].str.contains('Coalizione')]

    # Aggiungi il dataframe filtrato alla lista di dataframes
    dataframes.append(df_filt)

# Concatena tutti i dataframes della lista in un unico dataframe finale
df_finale = pd.concat(dataframes)
print(df_finale)

td_unique_values = df_finale['td'].unique().tolist()

lunga_stringa = ", ".join(td_unique_values)

# Inizializza una lista vuota per i valori separati
valori_separati = []

# Dividi la lunga stringa usando la virgola come separatore
valori_divisi = lunga_stringa.split(',')

# Estendi la lista dei valori separati con i valori divisi
valori_separati.extend(valori_divisi)
#print(valori_separati)
#print(df_finale)
# Stampa i valori separati

parole_chiave_da_rimuovere = [
    "con l'appoggio esterno di",
    "con ľappoggio esterno di",
    "con l'astensione di",
    "l'appoggio esterno di:",
    "con l'appoggio esterno del",
    "con l'appoggio esterno di:",
    "con l'appoggio esterno di:",
    "Appoggio esterno:",
    "e l'astensione di",
    "ľappoggio esterno di:",
    "l'appoggio esterno di",
    "l'appoggio esterno del"
]

# Crea una nuova lista di righe
nuove_righe = []

# Verifica se la colonna 'td' esiste nel DataFrame
if 'td' not in df_finale.columns:
    raise KeyError("Column 'td' not found in the DataFrame.")

# Itera su ogni riga del dataframe originale
for _, riga in df_finale.iterrows():
    coalizione = riga['th']
    link = riga['url']
    td = riga['td']
    
    # Separa i partiti usando la virgola come delimitatore
    partiti = td.split(',')
    
    # Rimuovi spazi vuoti iniziali e finali da ciascun partito
    partiti = [partito.strip() for partito in partiti]
    
    # Rimuovi numeri all'interno delle parentesi quadre
    partiti = [re.sub(r'\[\d+\]', '', partito) for partito in partiti]
    
    # Rimuovi frasi contenenti le parole chiave
    partiti = [re.sub(r'\bcon.*?\b', '', partito).strip() for partito in partiti]
    
    # Rimuovi "Appoggio esterno:" dai partiti
    partiti = [partito.replace("Appoggio esterno:", "").strip() for partito in partiti]
    
    # Rimuovi testo tra parentesi tonde
    partiti = [re.sub(r'\([^)]*\)', '', partito).strip() for partito in partiti]
    
    # Rimuovi valori basati su parole chiave
    partiti = [partito for partito in partiti if not any(parola_chiave in partito for parola_chiave in parole_chiave_da_rimuovere)]
    
    # Aggiungi ogni partito come una nuova riga nella lista
    for partito in partiti:
        nuove_righe.append({'Coalizione': coalizione, 'Partito': partito, 'Link': link})

# Crea un nuovo DataFrame con le nuove righe
df_separati = pd.DataFrame(nuove_righe)

# Stampa il DataFrame risultante
print(df_separati)

# Stampa il DataFrame risultante
#print(df_separati)

"""
parole_chiave_da_rimuovere = ["con l'appoggio esterno di", "con ľappoggio esterno di", "con l'astensione di", "l'appoggio esterno di:", "con l'appoggio esterno del", "con l'appoggio esterno di:", "con l'appoggio esterno di:", "Appoggio esterno:"]

# Rimuovi le parole chiave
valori_separati = [valore.strip() for valore in valori_separati]
valori_separati = [re.sub(r'\bcon.*?\b', '', valore).strip() for valore in valori_separati]
valori_separati = [valore.replace("Appoggio esterno:", "").strip() for valore in valori_separati]

# Rimuovi le date tra parentesi
valori_separati = [re.sub(r'\([^)]*\)', '', valore).strip() for valore in valori_separati]

# Rimuovi valori basati su parole chiave
valori_separati = [valore for valore in valori_separati if not any(parola_chiave in valore for parola_chiave in parole_chiave_da_rimuovere)]

valori_separati = list(set(valori_separati))
#print(valori_separati)
df_allineamento_partiti = pd.read_excel('Partitiii.xlsx')
print(df_allineamento_partiti.columns)
print(df_finale.columns)

df_finale['td'] = df_finale['td'].str.split(', ')
df_expanded = df_finale.explode('td')
df_expanded['td'] = df_expanded['td'].str.replace(r'\[\d+\]', '')
print(df_expanded)

# Effettua la fusione dei DataFrame in base alle colonne corrispondenti
#df_merged = df_finale.merge(df_allineamento_partiti, left_on='td', right_on='A')
# Stampa il risultato
#print(df_merged)

"""
