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

querydefinitivalaureadonne ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
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
df_laurea_donne = get(endpoint, querydefinitivalaureadonne)
df_laurea_donne = df_laurea_donne.drop_duplicates(["persona","nome", "cognome", "luogoNascita"]) #905 287 non laureate 569 laureate 49 non si sa 
df_donne_noinfo = df_laurea_donne[df_laurea_donne['info'].isna()] #49
df_laurea_donne['info'] = df_laurea_donne['info'].fillna('') 
masklaurea = df_laurea_donne['info'].str.contains('Laurea|laurea|Master|LAUREA')

# Estrarre le righe che soddisfano la maschera
laureate = df_laurea_donne[masklaurea]
laureate = laureate.assign(info="yes")
laureate = laureate.assign(gender='female')
laureate = laureate[["info", "gender"]]
donnelaureate = laureate.rename(columns={'info': 'graduated'})


masknonlaurea =~df_laurea_donne['info'].str.contains('Laurea|laurea|Master|LAUREA', na=False) & df_laurea_donne['info'].ne('')
donnenonlaureate = df_laurea_donne[masknonlaurea]
donnenonlaureate = donnenonlaureate.assign(info="no")
donnenonlaureate = donnenonlaureate.assign(gender='female')
donnenonlaureate = donnenonlaureate[["info", "gender"]]
donnenonlaureate = donnenonlaureate.rename(columns={'info': 'graduated'})

print(len(df_laurea_donne))
print(len(donnelaureate))
print(len(df_donne_noinfo))
print(len(donnenonlaureate))

#uominilaureacsv = pd.concat([uomininonlaureati, uominilaureati],  axis=0)
#uominilaureacsv.to_csv("mengraduation.csv",  index=False, index_label=False)
#print(len(uominilaureacsv)) #senza info sono 162, 3293 si, 1749

lista_donne_noinfo= df_donne_noinfo['nome'] + ' ' + df_donne_noinfo['cognome']
lista_donne_noinfo = lista_donne_noinfo.to_list()
lista_donne_noinfo= [nome_cognome.title() for nome_cognome in lista_donne_noinfo]
lista_donne_noinfo = [nome_cognome.replace(' ', '_') for nome_cognome in lista_donne_noinfo]

#CERCA PAGINA WIKIPEDIA DELLE PERSONE SENZA INFORMAZIONI SUL TITOLO DI STUDIO 

def check_url_exists(url):
    response = requests.head(url)
    return response.status_code == 200

url_lista = []
donne_con_url = []
donne_senza_url = []

for persona in lista_donne_noinfo:
    url_politico = f"https://it.wikipedia.org/wiki/{persona}_(politico)"
    url_generale = f"https://it.wikipedia.org/wiki/{persona}"
    
    if check_url_exists(url_politico):
        print(f"URL della pagina di Wikipedia per {persona} (politico): {url_politico}")
        url_lista.append(url_politico)
        donne_con_url.append(persona)
    elif check_url_exists(url_generale):
        print(f"URL della pagina di Wikipedia generica per {persona}: {url_generale}")
        url_lista.append(url_generale)
        donne_con_url.append(persona)
    else:
        print(f"Nessun URL trovato per {persona}")
        donne_senza_url.append(persona)

df_donne_con_url = pd.DataFrame({"Persona": donne_con_url, "URL": url_lista})
df_donne_senza_url = pd.DataFrame({"Persona": donne_senza_url})

print(len(df_donne_con_url))

#print(len(df_donne_con_url)) #36
print(len(df_donne_senza_url)) #13
#CODICE PER CERCARE SECONDI NOMI O ALTRI COGNOMI ECC. 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse

url_lista = []
donne_con_url2 = []

for persona in df_donne_senza_url["Persona"]:
    nome_cognome_parts = persona.split("_")  # Dividi la stringa utilizzando l'underscore come separatore
    nome = nome_cognome_parts[0]  # Primo nome
    cognome = nome_cognome_parts[-1]  # Ultimo cognome

    # Effettua la richiesta HTTP alla pagina di Wikipedia
    response = requests.get(f"https://it.wikipedia.org/wiki/{persona}", timeout=10)

    if response.status_code == 200:
        url_lista.append(response.url)
        donne_con_url2.append(persona)
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
                donne_con_url2.append(persona)
        else:
            print(f"Errore nella richiesta della pagina di Wikipedia per {persona}")

df_donne_con_url2 = pd.DataFrame({"Persona": donne_con_url2, "URL": url_lista})
print(len(df_donne_con_url2))
from fuzzywuzzy import fuzz

# Funzione per confrontare il nome della persona con l'URL in modo da controllare che non si prendano url diversi 
def confronta_nomi(nome, url):
    nome_splittato = nome.split('_')
    url_splittato = url.split('/')[-1].split('_')
    similarita = fuzz.token_set_ratio(nome_splittato, url_splittato)
    return similarita

# Filtra il dataframe per le righe che contengono il nome nella parte finale dell'URL
df_filtered = pd.DataFrame(columns=['Persona', 'URL'])  # DataFrame vuoto per i risultati filtrati
url_personali = set()

for index, row in df_donne_con_url2.iterrows():
    nome = row['Persona']
    url = row['URL']
    if confronta_nomi(nome, url) >= 90:  # Soglia di similarità più bassa
        if nome not in url_personali:
            df_filtered = df_filtered.append(row)
            url_personali.add(nome)

df_filtered.reset_index(drop=True, inplace=True)
print(len(df_filtered))

"""
#print(df_filtered)
print(df_donne_con_url2)
print(len(df_donne_con_url2))
print(len(df_filtered))
print(df_filtered)
"""

#UNISCO TUTTI I DATAFRAME CON GLI URL DI WIKIPEDIA DA CONTROLLARE 

df_controllo_wiki = pd.concat([df_filtered, df_donne_con_url]) #
#print(len(df_controllo_wiki))
#CONTROLLO CHE NELLE PAGINE DI WIKIPEDIA CI SIA LA SEZIONE TITOLO DI STUDIO 
urldaesaminare= df_controllo_wiki["URL"].tolist()

# Inizializza una lista per i dataframe
dataframes = []

# Itera sugli URL
for url in urldaesaminare:
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
df_filt = final_df[final_df['th'].str.contains('studio')]
#print(len(df_filt))

# Creazione dei due DataFrame vuoti
df_filt_con_laurea = pd.DataFrame(columns=df_filt.columns)
df_filt_senza_laurea = pd.DataFrame(columns=df_filt.columns)

# Iterazione sul DataFrame filtrato
for index, row in df_filt.iterrows():
    if 'laurea' in row['td'].lower():
        df_filt_con_laurea = df_filt_con_laurea.append(row)
    else:
        df_filt_senza_laurea = df_filt_senza_laurea.append(row)

# Reset dell'indice dei DataFrame risultanti
df_filt_con_laurea.reset_index(drop=True, inplace=True)
df_filt_senza_laurea.reset_index(drop=True, inplace=True)
#print(len(df_filt_con_laurea))
#print(len(df_filt_senza_laurea))
urlconsezionetitolodistudio = df_filt["url"].tolist()
#print(len(urlconsezionetitolodistudio))
valori_non_comuni = list(set(urldaesaminare) - set(urlconsezionetitolodistudio))
#print(valori_non_comuni)
#print(len(valori_non_comuni))

#url_lista_finale = dffinale["URL"].tolist()

df_con_parola = pd.DataFrame(columns=["Persona", "URL"])
df_senza_parola = pd.DataFrame(columns=["Persona", "URL"])

for url in valori_non_comuni:
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        h2_elements = soup.find_all("h2")

        found = False  # Flag per indicare se è stata trovata la parola "laurea", "laureò" o "laureata"

        for h2 in h2_elements:
            sibling_p = h2.find_next_sibling("p")
            while sibling_p:
                if re.search(r"\b(laurea|laureò|laureata|Laureatosi|laureatosi)\b", sibling_p.get_text(), re.IGNORECASE):
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
#print(df_con_parola)
print(len(df_con_parola))

print("Pagine senza nessuna delle parole:")
#print(df_senza_parola)
print(len(df_senza_parola))

#print(df_filt)


listacheckprofessione = df_senza_parola["URL"].tolist()

# Inizializza una lista per i dataframe
dataframes = []

# Itera sugli URL
for url in listacheckprofessione:
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
df_filtprofessione = final_df[final_df['th'].str.contains('Professione')]

dataframe_donne_laurea = pd.concat([donnelaureate, df_filt_con_laurea, df_con_parola]) #586
dataframe_donne_senza_laurea = pd.concat([donnenonlaureate, df_filt_senza_laurea]) #292
print(dataframe_donne_laurea)
print(len(dataframe_donne_laurea))
print(len(dataframe_donne_senza_laurea))