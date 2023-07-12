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
#print(df_ministri_legislature)
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
#print(df_governi)
lista_governi = df_governi["Governo"].to_list()
lista_governi.append("I Governo Meloni")

#lista_governi = lista_governi.append("Governo_Meloni")
#print(df_governi)
#print(lista_governi)

import re

nuova_lista = []

for governo in lista_governi:
    parti = governo.split()
    nuovo_nome = "Governo_" + "_".join(parti[2:]) + "_" + parti[0]
    nuova_lista.append(nuovo_nome)
#print(nuova_lista)
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
#print(df_finale)

td_unique_values = df_finale['td'].unique().tolist()

lunga_stringa = ", ".join(td_unique_values)

# Inizializza una lista vuota per i valori separati
valori_separati = []

# Dividi la lunga stringa usando la virgola come separatore
valori_divisi = lunga_stringa.split(',')

# Estendi la lista dei valori separati con i valori divisi
valori_separati.extend(valori_divisi)
#print(valori_separati)
#print(len(valori_separati))
print(df_finale)

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

    partiti = [re.sub(r'\bcon.*?\b', '', partito).strip() for partito in partiti]

    # Rimuovi testo tra parentesi tonde
    partiti = [re.sub(r'\([^)]*\)', '', partito).strip() for partito in partiti]

    
    # Rimuovi "Appoggio esterno:" dai partiti

    partiti = [partito.replace("con l'appoggio esterno del:", ",").strip() for partito in partiti]
    partiti = [partito.replace("con l'appoggio esterno di:", ",").strip() for partito in partiti]
    partiti = [partito.replace("con l'appoggio esterno di", ",").strip() for partito in partiti]
    partiti = [partito.replace("ľappoggio esterno di:", ",").strip() for partito in partiti]
    partiti = [partito.replace("Appoggio esterno:", ",").strip() for partito in partiti]
    partiti = [partito.replace("e l'astensione di:", ",").strip() for partito in partiti]
    partiti = [partito.replace("con l'appoggio esterno del", ",").strip() for partito in partiti]
    partiti = [partito.replace("l'appoggio esterno del", ",").strip() for partito in partiti]
    partiti = [partito.replace("l'appoggio esterno di:", ",").strip() for partito in partiti]
    partiti = [partito.replace("con l'astensione di:", ",").strip() for partito in partiti]
    partiti = [partito.replace("l'appoggio esterno di", ",").strip() for partito in partiti]

    partiti = [re.sub(r'\([^)]*\)', '', partito).strip() for partito in partiti]
    partiti = [re.sub(r'\[[^\]]*\]', '', partito).strip() for partito in partiti]
    partiti = [re.sub(r'[()\[\]]', '', partito).strip() for partito in partiti]
    
    #print(partiti)
    # Aggiungi ogni partito come una nuova riga nella lista
    for partito in partiti:
        if ',' in partito:
            partiti_divisi = partito.split(',')
            for partito_diviso in partiti_divisi:
                nuove_righe.append({'Coalizione': coalizione, 'Partito': partito_diviso.strip(), 'Link': link})
        else:
            nuove_righe.append({'Coalizione': coalizione, 'Partito': partito, 'Link': link})
    

# Crea un nuovo DataFrame con le nuove righe
df_separati = pd.DataFrame(nuove_righe)
#print(df_separati)
valori_da_eliminare = ["Indipendenti","Fareitalia", "RD", "USEI", "PeC", "AISA", "èV", "MAIE","Rin", "IaC", "NcI", "CI", "UdC", "MA", "SVP"]

# Rimuovi le righe che contengono i valori specificati nella colonna "Partito"
df_separati= df_separati[~df_separati['Partito'].isin(valori_da_eliminare)]
df_separati['Partito'] = df_separati['Partito'].str.replace('Ind.', '')
df_separati = df_separati.dropna(subset=['Partito'])
# Stampa il DataFrame risultante
#print(df_separati)

path_file_excel = 'PartitiFinito.xlsx'

# Leggi il file Excel e crea un DataFrame
df_partiti_finito = pd.read_excel(path_file_excel)
#print(df_partiti_finito)
#print(df_separati)
df_merge = df_separati.merge(df_partiti_finito, left_on="Partito", right_on="A", how="left")
df_merge = df_merge.drop('Unnamed: 3', axis=1)
df_merge = df_merge.drop('Unnamed: 4', axis=1)
df_merge = df_merge.drop('A', axis=1)
df_merge = df_merge.drop('B', axis=1)
df_merge = df_merge.drop('Coalizione', axis=1)
df_merge['Link'] = df_merge['Link'].str.split('wiki/').str[1]
df_merge = df_merge.rename(columns={'Link': 'Governo', 'C': 'Allineamento'})
print(df_merge)
#conto i valori presenti nella colonna allineamento
alignment_counts = df_merge.groupby('Governo')['Allineamento'].value_counts().unstack().fillna(0)
alignment_result = alignment_counts.idxmax(axis=1)
result_df = pd.DataFrame({'Governo': alignment_result.index, 'Allineamento Risultante': alignment_result.values})
#print(result_df)
#print(result_df)# Stampa il DataFrame risultante
#values_not_in_common = pd.concat([result_df['Governo'], df_governi['Governo']]).drop_duplicates(keep=False) 
#print(result_df)
#print(len(result_df))
#print(df_governi)
#print(len(df_governi))
#print(df_merge)
unique_values = df_merge['Partito'].unique()
num_unique_values = len(unique_values)
#print(len(unique_values))
#print(df_finale)
#print(values_not_in_common)
#print(df_separati.columns)
#print(df_separati)
#print(num_unique_values)


