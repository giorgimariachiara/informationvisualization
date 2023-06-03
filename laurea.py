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

querydefinitivalaureauomini ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
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
df_laurea_uomini = get(endpoint, querydefinitivalaureauomini)
df_laurea_uomini = df_laurea_uomini.drop_duplicates(["persona","nome", "cognome", "luogoNascita"]) #5204
df_uomini_noinfo = df_laurea_uomini[df_laurea_uomini['info'].isna()] #162
df_laurea_uomini['info'] = df_laurea_uomini['info'].fillna('') 
masklaurea = df_laurea_uomini['info'].str.contains('Laurea|laurea|Master|LAUREA')

# Estrarre le righe che soddisfano la maschera
laureati = df_laurea_uomini[masklaurea]
laureati = laureati.assign(info="yes")
laureati = laureati.assign(gender='male')
laureati = laureati[["info", "gender"]]
uominilaureati = laureati.rename(columns={'info': 'graduated'})


masknonlaurea =~df_laurea_uomini['info'].str.contains('Laurea|laurea|Master|LAUREA', na=False) & df_laurea_uomini['info'].ne('')
uomininonlaureati = df_laurea_uomini[masknonlaurea]
uomininonlaureati = uomininonlaureati.assign(info="no")
uomininonlaureati = uomininonlaureati.assign(gender='male')
uomininonlaureati = uomininonlaureati[["info", "gender"]]
uomininonlaureati = uomininonlaureati.rename(columns={'info': 'graduated'})
"""
print(len(df_laurea_uomini))
print(len(uominilaureati))
print(len(df_uomini_noinfo))
print(len(uomininonlaureati))
"""
#uominilaureacsv = pd.concat([uomininonlaureati, uominilaureati],  axis=0)
#uominilaureacsv.to_csv("mengraduation.csv",  index=False, index_label=False)
#print(len(uominilaureacsv)) #senza info sono 162, 3293 si, 1749

lista_uomini_noinfo= df_uomini_noinfo['nome'] + ' ' + df_uomini_noinfo['cognome']
lista_uomini_noinfo = lista_uomini_noinfo.to_list()
lista_uomini_noinfo= [nome_cognome.title() for nome_cognome in lista_uomini_noinfo]
lista_uomini_noinfo = [nome_cognome.replace(' ', '_') for nome_cognome in lista_uomini_noinfo]

#CERCA PAGINA WIKIPEDIA DELLE PERSONE SENZA INFORMAZIONI SUL TITOLO DI STUDIO 

def check_url_exists(url):
    response = requests.head(url)
    return response.status_code == 200

url_lista = []
uomini_con_url = []
uomini_senza_url = []

for persona in lista_uomini_noinfo:
    url_politico = f"https://it.wikipedia.org/wiki/{persona}_(politico)"
    url_generale = f"https://it.wikipedia.org/wiki/{persona}"
    
    if check_url_exists(url_politico):
        print(f"URL della pagina di Wikipedia per {persona} (politico): {url_politico}")
        url_lista.append(url_politico)
        uomini_con_url.append(persona)
    elif check_url_exists(url_generale):
        print(f"URL della pagina di Wikipedia generica per {persona}: {url_generale}")
        url_lista.append(url_generale)
        uomini_con_url.append(persona)
    else:
        print(f"Nessun URL trovato per {persona}")
        uomini_senza_url.append(persona)

df_uomini_con_url = pd.DataFrame({"Persona": uomini_con_url, "URL": url_lista})
df_uomini_senza_url = pd.DataFrame({"Persona": uomini_senza_url})

#print(len(df_uomini_con_url))
#print(len(df_uomini_senza_url))
#CODICE PER CERCARE SECONDI NOMI O ALTRI COGNOMI ECC. 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse

url_lista = []
uomini_con_url2 = []

for persona in df_uomini_senza_url["Persona"]:
    nome_cognome_parts = persona.split("_")  # Dividi la stringa utilizzando l'underscore come separatore
    nome = nome_cognome_parts[0]  # Primo nome
    cognome = nome_cognome_parts[-1]  # Ultimo cognome

    # Effettua la richiesta HTTP alla pagina di Wikipedia
    response = requests.get(f"https://it.wikipedia.org/wiki/{persona}", timeout=10)

    if response.status_code == 200:
        url_lista.append(response.url)
        uomini_con_url2.append(persona)
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
                uomini_con_url2.append(persona)
        else:
            print(f"Errore nella richiesta della pagina di Wikipedia per {persona}")

df_uomini_con_url2 = pd.DataFrame({"Persona": uomini_con_url2, "URL": url_lista})
#print(df_uomini_con_url2)
pattern = r"\b(" + "|".join(df_uomini_con_url2['Persona'].str.replace('_', ' ')) + r")\b"
df_filtered = df_uomini_con_url2[df_uomini_con_url2['URL'].str.contains(pattern, case=False)]


#print(df_filtered)
#print(df_uomini_con_url2)
nomi_da_cercare = df_uomini_con_url2["Persona"].tolist()

df_filtered_list = []

for nome_cognome in nomi_da_cercare:
    # Filtra il DataFrame per le righe che contengono il nome e cognome nell'URL
    df_filtered = df_uomini_con_url2[df_uomini_con_url2["URL"].str.contains(nome_cognome.replace(" ", "_"))]
    
    # Aggiungi il DataFrame filtrato alla lista solo se contiene righe
    if not df_filtered.empty:
        df_filtered_list.append(df_filtered)

# Unisci tutti i DataFrame filtrati nella lista
df_final = pd.concat(df_filtered_list)
df_final = df_final.drop_duplicates(subset='Persona')

# Stampa il DataFrame finale
#print(df_final)
# DataFrame vuoto per i risultati
#df_risultati = pd.DataFrame(columns=["Persona", "URL"])

#UNISCO TUTTI I DATAFRAME CON GLI URL DI WIKIPEDIA DA CONTROLLARE 

df_controllo_wiki = pd.concat([df_final, df_uomini_con_url]) #134 
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

"""
# Stampa dei DataFrame
print("Pagine con almeno una delle parole:")
print(df_con_parola)
print(len(df_con_parola))

print("Pagine senza nessuna delle parole:")
print(df_senza_parola)
print(len(df_senza_parola))

#print(df_filt)

"""
    
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

print(df_filtprofessione)
print(len(df_filtprofessione))
