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
?d foaf:surname ?cognome; foaf:gender "male" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.
}}"""
df_laurea_uomini = get(endpoint, querydefinitivalaureauomini)
df_laurea_uomini = df_laurea_uomini.drop_duplicates(["persona","nome", "cognome", "luogoNascita"]) #5204
df_laurea_uomini_data = df_laurea_uomini.drop_duplicates(["persona","nome", "cognome", "dataNascita","luogoNascita"])
#print(len(df_laurea_uomini_data))
#df_laurea_uomini = df_laurea_uomini.sort_values(by='cognome')
row_margiotta = df_laurea_uomini[df_laurea_uomini['cognome'] == 'LAVORATO']
print(row_margiotta)
df_uomini_noinfo = df_laurea_uomini[df_laurea_uomini['info'].isna()] 
#df_uomini_noinfo = df_uomini_noinfo[["nome", "cognome", "luogoNascita"]]
df_uomini_noinfo_data = df_uomini_noinfo[["nome", "cognome", "dataNascita"]]

import locale
from datetime import datetime

# Imposta la localizzazione italiana
locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')

lista_politici_data = []

def capitalize_name(name):
    parts = re.split(r"([ '-])", name)
    return "".join([part.capitalize() for part in parts])

for index, row in df_uomini_noinfo_data.iterrows():
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

print(lista_politici_data)

lista_politici = []

for index, row in df_uomini_noinfo.iterrows():
    nome_cognome = f"{row['nome'].title()}_{row['cognome'].title()}"
    citta_nascita = row['luogoNascita']
    if pd.notna(citta_nascita):
        citta_nascita = citta_nascita.title().replace("_", " ")
    lista_politici.append((nome_cognome, citta_nascita))

#print(lista_politici)
#print(len(lista_politici))
df_laurea_uomini['info'] = df_laurea_uomini['info'].fillna('') 
masklaurea = df_laurea_uomini['info'].str.contains('Laurea|laurea|Master|LAUREA')

# Estrarre le righe che soddisfano la maschera
laureati = df_laurea_uomini[masklaurea]
#laureati = laureati.assign(info="yes")
laureati = laureati.assign(gender='male')
uominilaureati = laureati[["nome", "cognome", "gender"]]
#uominilaureati = laureati.rename(columns={'info': 'graduated'})


masknonlaurea =~df_laurea_uomini['info'].str.contains('Laurea|laurea|Master|LAUREA', na=False) & df_laurea_uomini['info'].ne('')
uomininonlaureati = df_laurea_uomini[masknonlaurea]
#uomininonlaureati = uomininonlaureati.assign(info="no")
uomininonlaureati = uomininonlaureati.assign(gender='male')
uomininonlaureati = uomininonlaureati[["nome", "cognome", "gender"]]
#uomininonlaureati = uomininonlaureati.rename(columns={'info': 'graduated'})

#print(len(df_laurea_uomini)) #5204
#print(uominilaureati) #3293
#print(uominilaureati)
#print(len(df_uomini_noinfo)) #162
#print(uomininonlaureati) #1749
#print(len(uomininonlaureati))

#uominilaureacsv = pd.concat([uomininonlaureati, uominilaureati],  axis=0)
#uominilaureacsv.to_csv("mengraduation.csv",  index=False, index_label=False)
#print(len(uominilaureacsv)) #senza info sono 162, 3293 si, 1749

lista_uomini_noinfo= df_uomini_noinfo['nome'] + ' ' + df_uomini_noinfo['cognome'] 
#print(lista_uomini_noinfo)
lista_uomini_noinfo = lista_uomini_noinfo.to_list()
lista_uomini_noinfo= [nome_cognome.title() for nome_cognome in lista_uomini_noinfo]
lista_uomini_noinfo = [nome_cognome.replace(' ', '_') for nome_cognome in lista_uomini_noinfo]
#print(lista_uomini_noinfo)
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
uomini_con_url = []
uomini_senza_url = []

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
                        print(f"URL della pagina di Wikipedia per {nome_cognome} (politico con data): {url_politico_data}")
                        url_lista.append(url_politico_data)
                        uomini_con_url.append(nome_cognome)
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
                        print(f"URL della pagina di Wikipedia per {nome_cognome} (politico): {url_politico}")
                        url_lista.append(url_politico)
                        uomini_con_url.append(nome_cognome)
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
                        print(f"URL della pagina di Wikipedia generica per {nome_cognome}: {url_generale}")
                        url_lista.append(url_generale)
                        uomini_con_url.append(nome_cognome)
                        continue
                else:
                    print("Paragrafo non trovato.")
        print(f"Nessun URL trovato per {nome_cognome}")
        
        uomini_senza_url.append((nome_cognome, data_nascita))

df_uomini_con_url = pd.DataFrame({"Persona": uomini_con_url, "URL": url_lista})
df_uomini_senza_url = pd.DataFrame({"Persona e Data di nascita": uomini_senza_url})
print("uomini con url")
print(len(df_uomini_con_url))
print("uomini senza url")
print(len(df_uomini_senza_url))

"""
def check_url_exists(url):
    response = requests.get(url)
    return response.status_code == 200


url_lista = []
uomini_con_url = []
uomini_senza_url = []

for persona, citta_nascita in lista_politici:
    url_politico_data = None
    for anno_iniziale in [1952, 1940, 1929]:
        url_politico_data = f"https://it.wikipedia.org/wiki/{persona}_(politico_{anno_iniziale})"
        if check_url_exists(url_politico_data):
            pagina_wikipedia = requests.get(url_politico_data)
            if pagina_wikipedia.status_code == 200:
                soup = BeautifulSoup(pagina_wikipedia.text, 'html.parser')
                div_parser_output = soup.find('div', class_='mw-parser-output')
                if div_parser_output:
                    paragraph = div_parser_output.find('p')
                    if paragraph:
                        city_link = paragraph.find('a')
                        if city_link and city_link.get('title', '').lower() == citta_nascita.lower():
                            print(f"URL della pagina di Wikipedia per {persona} (politico con data): {url_politico_data}")
                            url_lista.append(url_politico_data)
                            uomini_con_url.append(persona)
                            break
                else:
                    print("Div mw-parser-output non trovato.")
    else:
        url_politico = f"https://it.wikipedia.org/wiki/{persona}_(politico)"
        url_generale = f"https://it.wikipedia.org/wiki/{persona}"
        if check_url_exists(url_politico):
            pagina_wikipedia = requests.get(url_politico)
            if pagina_wikipedia.status_code == 200:
                soup = BeautifulSoup(pagina_wikipedia.text, 'html.parser')
                div_parser_output = soup.find('div', class_='mw-parser-output')
                if div_parser_output:
                    paragraph = div_parser_output.find('p')
                    if paragraph:
                        city_link = paragraph.find('a')
                        if city_link and city_link.get('title', '').lower() == citta_nascita.lower():
                            print(f"URL della pagina di Wikipedia per {persona} (politico): {url_politico}")
                            url_lista.append(url_politico)
                            uomini_con_url.append(persona)
                            continue
                else:
                    print("Div mw-parser-output non trovato.")
        if check_url_exists(url_generale):
            pagina_wikipedia = requests.get(url_generale)
            if pagina_wikipedia.status_code == 200:
                soup = BeautifulSoup(pagina_wikipedia.text, 'html.parser')
                div_parser_output = soup.find('div', class_='mw-parser-output')
                if div_parser_output:
                    paragraph = div_parser_output.find('p')
                    if paragraph:
                        city_link = paragraph.find('a')
                        if city_link and city_link.get('title', '').lower() == citta_nascita.lower():
                            print(f"URL della pagina di Wikipedia generica per {persona}: {url_generale}")
                            url_lista.append(url_generale)
                            uomini_con_url.append(persona)
                            continue
                else:
                    print("Div mw-parser-output non trovato.")
        print(f"Nessun URL trovato per {persona}")
        uomini_senza_url.append(persona)

df_uomini_con_url = pd.DataFrame({"Persona": uomini_con_url, "URL": url_lista})
df_uomini_senza_url = pd.DataFrame({"Persona": uomini_senza_url})

print(len(df_uomini_con_url))
print(len(df_uomini_senza_url))
"""
#print(df_uomini_senza_url)
uomin= df_uomini_senza_url['Persona e Data di nascita'].values.tolist()


import pandas as pd
import wikipediaapi
import re

def extract_birth_date(text):
    matches = re.findall(r'\d+\s+\w+\s+\d+', text)
    if matches:
        birth_date = matches[0]
        return birth_date.strip()
    return None

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

for item in uomin:
    nome_cognome, data_nascita, url = find_wikipedia_url(item[0], item[1])
    if url:
        df_with_url = pd.concat([df_with_url, pd.DataFrame({'Nome': [nome_cognome], 'Data di nascita': [data_nascita], 'URL': [url]})], ignore_index=True)
    else:
        df_without_url = pd.concat([df_without_url, pd.DataFrame({'Nome': [nome_cognome], 'Data di nascita': [data_nascita]})], ignore_index=True)

print("DataFrame con URL:")

print("DataFrame con URL:")
print(len(df_with_url))
print("\nDataFrame senza URL:")
print(len(df_without_url))


"""
def check_url_exists(url):
    response = requests.head(url)
    return response.status_code == 200

url_lista = []
uomini_con_url = []
uomini_senza_url = []

for persona in lista_uomini_noinfo:
    url_politico = f"https://it.wikipedia.org/wiki/{persona}_(politico)"
    url_generale = f"https://it.wikipedia.org/wiki/{persona}"
    
    url_politico_data = None
    for anno_iniziale in [1983, 1940, 1929]:
        url_politico_data = f"https://it.wikipedia.org/wiki/{persona}_(politico_{anno_iniziale})"
        if check_url_exists(url_politico_data):
            print(f"URL della pagina di Wikipedia per {persona} (politico con data): {url_politico_data}")
            url_lista.append(url_politico_data)
            uomini_con_url.append(persona)
            break
    else:  # Eseguito solo se il loop precedente non ha raggiunto il break
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

df_uomini_con_url3 = pd.DataFrame({"Persona": uomini_con_url, "URL": url_lista})
df_uomini_senza_url3 = pd.DataFrame({"Persona": uomini_senza_url})

print(len(df_uomini_con_url3))
print(len(df_uomini_senza_url3))
uomin3 = df_uomini_con_url3["Persona"].to_list()

listaaa = list(set(uomin) ^ set(uomin3))
print(listaaa)
"""
#CODICE PER CERCARE SECONDI NOMI O ALTRI COGNOMI ECC. 
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
pattern = r"\b(" + "|".join(df_uomini_con_url2['Persona'].str.replace('_', ' ')) + r")\b"
df_filtered = df_uomini_con_url2[df_uomini_con_url2['URL'].str.contains(pattern, case=False)]
#print(df_filtered)
#print(df_uomini_con_url2)
nomi_da_cercare = df_uomini_con_url2["Persona"].tolist()

#questo serve per non far prendere url con solo tipo il cognome ecc qui prende infatti solo ludovico 
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
print(df_final)
print(len(df_final))
# DataFrame vuoto per i risultati
#df_risultati = pd.DataFrame(columns=["Persona", "URL"])

#UNISCO TUTTI I DATAFRAME CON GLI URL DI WIKIPEDIA DA CONTROLLARE 

df_controllo_wiki = pd.concat([df_final, df_uomini_con_url]) #133
print(len(df_controllo_wiki))
#CONTROLLO CHE NELLE PAGINE DI WIKIPEDIA CI SIA LA SEZIONE TITOLO DI STUDIO 
urldaesaminare= df_controllo_wiki["URL"].tolist()
#print(urldaesaminare)
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
# Creazione dei due DataFrame vuoti
df_filt_con_laurea = pd.DataFrame(columns=df_filt.columns)
df_filt_senza_laurea = pd.DataFrame(columns=df_filt.columns)

# Iteration over the filtered DataFrame
for index, row in df_filt.iterrows():
    if 'laurea' in row['td'].lower():
        df_filt_con_laurea = pd.concat([df_filt_con_laurea, row.to_frame().transpose()], ignore_index=True)
    else:
        df_filt_senza_laurea = pd.concat([df_filt_senza_laurea, row.to_frame().transpose()], ignore_index=True)

df_filt_con_laurea['nome'] = df_filt_con_laurea['url'].str.split('/').str[-1].str.split('_').str[:-1].str.join(' ')
df_filt_con_laurea['cognome'] = df_filt_con_laurea['url'].str.split('/').str[-1].str.split('_').str[-1]
df_filt_con_laurea = df_filt_con_laurea.assign(gender='male')
df_filt_con_laurea = df_filt_con_laurea[['nome', 'cognome', 'gender']]

#print(len(df_filt_con_laurea))
df_filt_senza_laurea['nome'] = df_filt_senza_laurea['url'].str.split('/').str[-1].str.split('_').str[:-1].str.join(' ')
df_filt_senza_laurea['cognome'] = df_filt_senza_laurea['url'].str.split('/').str[-1].str.split('_').str[-1]
df_filt_senza_laurea = df_filt_senza_laurea.assign(gender='male')
df_filt_senza_laurea = df_filt_senza_laurea[['nome', 'cognome', 'gender']]
#print(len(df_filt_senza_laurea))
#print(len(df_filt_con_laurea))
#print(df_filt_con_laurea)
#print(df_filt_senza_laurea)

urlconsezionetitolodistudio = df_filt["url"].tolist()
#print(len(urlconsezionetitolodistudio))
valori_non_comuni = list(set(urldaesaminare) - set(urlconsezionetitolodistudio))
#print(valori_non_comuni)
print(len(valori_non_comuni)) #url senza sezione titolo di studio   70

#CONTROLLO DEGLI URL SENZA LA SEZIONE TITOLO DI STUDIO SE HANNO INFO LAUREA NELLA BIO 

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
                if re.search(r"\b(laurea|laureò|laureato|Laureatosi|laureatosi)\b", sibling_p.get_text(), re.IGNORECASE):
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

#creo di nuovo una colonna nome, cognome e url per il dataframe finale 
df_con_parola['nome'] = df_con_parola['Persona'].str.split('_').str[0]
df_con_parola['cognome'] = df_con_parola['Persona'].str.split('_').str[1]
df_con_parola = df_con_parola.assign(gender='male')
df_con_parola = df_con_parola[['nome', 'cognome', 'gender', 'URL']]

df_senza_parola['nome'] = df_senza_parola['Persona'].str.split('_').str[0]
df_senza_parola['cognome'] = df_senza_parola['Persona'].str.split('_').str[1]
df_senza_parola = df_senza_parola.assign(gender='male')
df_senza_parola = df_senza_parola[['nome', 'cognome', 'gender', 'URL']]
# Stampa dei DataFrame
print("Pagine con almeno una delle parole:")
#print(df_con_parola)
print(len(df_con_parola))

print("Pagine senza nessuna delle parole:")
#print(df_senza_parola)
print(len(df_senza_parola))
#print(df_filt)

listacheckprofessione = df_senza_parola["URL"].tolist()
#print(len(listacheckprofessione))
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
#print(final_df)
#final_df = final_df.drop_duplicates(subset=['url'])
#print(len(final_df))
df_filtprofessione = final_df[final_df['th'].str.contains('Professione')]
print(len(df_filtprofessione))

df_da_escludere = df_filtprofessione[["url"]]
url_senza_professione = final_df[~final_df['url'].isin(df_da_escludere['url'])]
url_senza_professione = url_senza_professione.drop_duplicates(subset=['url'])
prova= pd.concat([df_filtprofessione, url_senza_professione])
listacheck = prova["url"].tolist()
lista3 = list(set(listacheckprofessione) ^ set(listacheck))
#print(df_senzaprofessione)
print(lista3)
print(len(url_senza_professione))
#print(url_senza_professione)
#print(len(df_filtprofessione))
#print(df_filtprofessione)

df_avvocato_professore = pd.DataFrame(columns=df.columns)
df_ingegnere = pd.DataFrame(columns=df.columns)
df_altro = pd.DataFrame(columns=df.columns)

# Iteration over the original DataFrame
for index, row in df_filtprofessione.iterrows():
    professione = row['td'].lower()
    if 'avvocato' in professione or 'docente universitario' in professione:
        df_avvocato_professore = pd.concat([df_avvocato_professore, row.to_frame().transpose()], ignore_index=True)
    elif 'ingegnere' in professione:
        df_ingegnere = pd.concat([df_ingegnere, row.to_frame().transpose()], ignore_index=True)
    else:
        df_altro = pd.concat([df_altro, row.to_frame().transpose()], ignore_index=True)

# Stampa dei tre DataFrame risultanti
print("DataFrame Avvocato/Professore Universitario:")
print(len(df_avvocato_professore))
print("DataFrame altro:")
print(len(df_altro))
# Estrazione del nome e del cognome dall'URL
df_avvocato_professore['nome'] = df_avvocato_professore['url'].str.split('/').str[-1].str.replace('_', ' ')
df_avvocato_professore['cognome'] = df_avvocato_professore['nome'].str.split().str[-1]
df_avvocato_professore['nome'] = df_avvocato_professore['nome'].str.split().str[0]
df_avvocato_professore = df_avvocato_professore.assign(gender='male')
df_avvocato_professore = df_avvocato_professore[['nome', 'cognome', 'gender']]
print("DataFrame Ingegnere:")
print(len(df_ingegnere))
#print()

#print("DataFrame Altro:")
#print(df_altro)
df_altro['nome'] = df_altro['url'].str.split('/').str[-1].str.replace('_', ' ')
df_altro['cognome'] = df_altro['nome'].str.split().str[-1]
df_altro['nome'] = df_altro['nome'].str.split().str[0]
df_altro = df_altro.assign(gender='male')
df_altro= df_altro[['nome', 'cognome', 'gender']]

df_con_parola = df_con_parola[['nome', 'cognome', 'gender']]


uominilaureati_f = pd.concat([df_filt_con_laurea, uominilaureati, df_con_parola, df_avvocato_professore])

#print(len(uominilaureati_f))

#print(df_con_parola)
#print(len(df_avvocato_professore))
#print(uominilaureati_f)
print("Uomini laureati totale:")
print(len(uominilaureati_f))

#print(uominilaureati_f)
#print(len(uominilaureati))
#print(len(df_con_parola))
#print(len(df_avvocato_professore))
#print(len(df_filt_con_laurea))
#print(len(uominilaureati_f))

#print(df_filt_con_laurea.columns) #nome cognome gender
#print(uominilaureati.columns) #nome cognome gender
#print(df_con_parola.columns) #nome cognome gender
#print(len(uominilaureati_f))
#print(df_uomini_senza_url)
uomininonlaureati_f = pd.concat([df_filt_senza_laurea, uomininonlaureati, df_altro])
df_uomini_senza_url['nome'] = df_uomini_senza_url['Persona'].str.split('_').str[0]
df_uomini_senza_url['cognome'] = df_uomini_senza_url['Persona'].str.split('_').str[1]
df_uomini_senza_url = df_uomini_senza_url[["nome", "cognome"]]
df_uomini_senza_url = df_uomini_senza_url.assign(gender='male')
#print(df_uomini_senza_url)

#uomininoinfo_f = pd.concat([df_altro, df_uomini_senza_url])
#print(uomininoinfo_f.columns)
#print(len(uomininoinfo_f))
print("Uomini non laureati totale:")
print(len(uomininonlaureati_f))
#print(df_filtprofessione)
#print(len(df_filtprofessione))
#print(df_filtprofessione)

#misà che dobbiamo lasciare i nomi con il _ e li dobbiamo confrontare così 