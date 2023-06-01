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

#ripuliamo il precedente dataframe prendendo solo gli url con effettivamente almeno il nome e cognome delle persone 
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
                    persona = os.path.basename(url)
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
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Lista degli URL da analizzare
urls = [
    "https://it.wikipedia.org/wiki/Maria_Maddalena_Rossi",
    "https://it.wikipedia.org/wiki/Michele_Troisi",
    "https://it.wikipedia.org/wiki/Michele_Troisi"
]

# Inizializza una lista per i dataframe
dataframes = []

# Itera sugli URL
for url in urls:
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
    
    # Aggiungi il dataframe alla lista
    dataframes.append(df)

# Concatena tutti i dataframe in uno unico
final_df = pd.concat(dataframes, ignore_index=True)
df_filtered = final_df.loc[final_df['th'] == 'Università']
# Stampa il dataframe finale
#print(final_df)
#print(type(final_df))
print(df_filtered)




"""
#QUESTO FUNZIONA 
import requests
from bs4 import BeautifulSoup

# URL della pagina da cui estrarre le informazioni
url = "https://it.wikipedia.org/wiki/Partito_Comunista_d%27Italia_(marxista-leninista)"

# Effettua la richiesta GET alla pagina
response = requests.get(url)

# Verifica che la richiesta abbia avuto successo
if response.status_code == 200:
    # Crea l'oggetto BeautifulSoup per analizzare l'HTML
    soup = BeautifulSoup(response.content, "html.parser")

    # Trova la tabella desiderata in base alla classe CSS
    table = soup.find("table", class_="sinottico")

    # Inizializza la variabile valore con un valore predefinito
    valore = "Valore non trovato"

    # Trova la riga corrispondente alla sezione "Collocazione"
    rows = table.find_all("tr")
    for row in rows:
        # Trova la cella con il valore "Collocazione"
        if "Collocazione" in row.text:
            # Estrai il valore "centro" o "Centro" indipendentemente dalla case
            cells = row.find_all("td")
            for cell in cells:
                if "centro" in cell.text.lower():
                    valore = cell.text.strip()

    # Stampa il valore estratto
    print("Valore estratto:", valore)
else:
    print("Errore nella richiesta HTTP")
"""
