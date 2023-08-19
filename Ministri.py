import pandas as pd
import requests
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import time


from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe
from sparql_dataframe import get
pd.set_option('display.max_rows', None)
endpoint = "https://dati.camera.it/sparql"
def getdatafromwiki(query):
    url = 'https://query.wikidata.org/sparql'
    r = requests.get(url, params={'format': 'json', 'query': query})
    data = r.json()
    bindings = data['results']['bindings']
    persons = [(binding['person']['value'], binding['nameLabel']['value'], binding['surnameLabel']['value'], binding['positionLabel']['value']) for binding in bindings]
    return persons

# Esempio di utilizzo
query = '''
SELECT ?person ?nameLabel ?surnameLabel ?positionLabel WHERE {
  ?person wdt:P31 wd:Q5;
          wdt:P735 ?name;
          wdt:P734 ?surname.
  {
    ?person p:P39 ?statement.
    ?statement ps:P39 ?position.
    ?position wdt:P279* wd:Q83307.
    ?position rdfs:label ?positionLabel.
    FILTER(LANG(?positionLabel) = "it").
    FILTER(?position IN (wd:Q27999166, wd:Q55044982, wd:Q33125109, wd:Q114953887, wd:Q55045585, wd:Q55053641, wd:Q55045454, wd:Q105541338, wd:Q55024178, wd:Q27991492, wd:Q1541071, wd:Q25973167, wd:Q1538785, wd:Q28002382, wd:Q27991508, wd:Q658082, wd:Q32137240, wd:Q6092845, wd:Q28000241, wd:Q34153264, wd:Q54879307, wd:Q55049421, wd:Q27795992, wd:Q55096606)).
  }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
    ?name rdfs:label ?nameLabel.
    ?surname rdfs:label ?surnameLabel.
  }
}
'''

persons = getdatafromwiki(query)

# Creazione del DataFrame
df = pd.DataFrame(persons, columns=['URI', 'Name', 'Surname', 'Position'])
#print(df)
#print(len(df.drop_duplicates()))

#INFORMAZIONI SULLE DIVERSE LEGISLATURE E I LORO INIZI E FINE 
querylegislature = """SELECT DISTINCT ?legislatura ?start ?end 
 
WHERE { ?legislatura rdf:type ocd:legislatura;
                     ocd:startDate ?start;
                     ocd:endDate ?end. 
} """


df_info_lesiglature = get(endpoint, querylegislature)
#print(df_info_lesiglature)

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
df_ministri_legislature["governoLabel"] = df_ministri_legislature["governoLabel"].str.split(" ", n=1).str[0]

#print(df_ministri_legislature)
#print(len(df_ministri_legislature))


#QUERY PER TROVARE PARTITO 
query_partito_uomini1 = """SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?gruppoPar
WHERE {
  ?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
  
  ?d a ocd:deputato;
     ocd:rif_leg ?legislatura;
     ocd:rif_mandatoCamera ?mandato.
  ?d ocd:aderisce ?gruppo.
  ?gruppo rdfs:label ?gruppoPar.
  
  ## Anagrafica
  ?d foaf:surname ?cognome; foaf:gender "male"; foaf:firstName ?nome.
  
  OPTIONAL {
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
    rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
  }
  
  FILTER (?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/costituente>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>)
}
"""

query_partito_uomini2 = """SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?gruppoPar
WHERE {
  ?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
  
  ?d a ocd:deputato;
     ocd:rif_leg ?legislatura;
     ocd:rif_mandatoCamera ?mandato.
  ?d ocd:aderisce ?gruppo.
  ?gruppo rdfs:label ?gruppoPar.
  
  ## Anagrafica
  ?d foaf:surname ?cognome; foaf:gender "male"; foaf:firstName ?nome.
  
  OPTIONAL {
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
    rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
  }
  
  FILTER (
             ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>)
}
"""
query_partito_donne = """SELECT DISTINCT ?persona ?cognome ?nome
?luogoNascita ?gruppoPar
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

?d a ocd:deputato; 
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.
OPTIONAL{?d dc:description ?info}
?d ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.

}}"""
#dataprova1 = dataprova[["nome", "cognome"]]
df_partito_donne = get(endpoint, query_partito_donne)
df_partito_donne['gruppoPar'] = df_partito_donne['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_donne = df_partito_donne.drop_duplicates(["persona","nome", "cognome", "luogoNascita", "gruppoPar"])
df_partito_donne = df_partito_donne[["nome", "cognome", "gruppoPar"]]
df_partito_donne = df_partito_donne[["gruppoPar"]]
df_partito_donne.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_donne = df_partito_donne.assign(gender='female')
#print(len(df_partito_donne))

df_partito_uomini1 = get(endpoint, query_partito_uomini1)
df_partito_uomini1['gruppoPar'] = df_partito_uomini1['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_uomini1 = df_partito_uomini1.drop_duplicates(["persona","nome", "cognome", "luogoNascita", "gruppoPar"])
df_partito_uomini1 = df_partito_uomini1[["gruppoPar"]]
df_partito_uomini1.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_uomini1 = df_partito_uomini1.assign(gender='male')

df_partito_uomini2 = get(endpoint, query_partito_uomini2)
df_partito_uomini2['gruppoPar'] = df_partito_uomini2['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_uomini2 = df_partito_uomini2.drop_duplicates(["persona","nome", "cognome", "luogoNascita", "gruppoPar"])
df_partito_uomini2 = df_partito_uomini2[["gruppoPar"]]
df_partito_uomini2.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_uomini2 = df_partito_uomini2.assign(gender='male')

df_partito_totale = pd.concat([df_partito_uomini1, df_partito_uomini2, df_partito_donne])
df_partito_totale = df_partito_totale[["partito"]].drop_duplicates()
#print(df_partito_totale)
#print(len(df_partito_totale))
listapartiti = df_partito_totale["partito"].tolist()
#print(listapartiti)

import pandas as pd

# Leggi il file Excel e crea il DataFrame
df = pd.read_excel('Partiti.xlsx')
listapartiti = [partito.lower().strip() for partito in listapartiti]
df['A'] = df['A'].str.lower().str.strip()

# Crea un dizionario dalla colonna A alla colonna B del DataFrame
mappa_partiti = dict(zip(df['A'], df['B']))

# Itera sulla lista dei partiti
for i in range(len(listapartiti)):
    partito = listapartiti[i]
    if partito in mappa_partiti:
        listapartiti[i] = mappa_partiti[partito]
listapartiti = list(set(listapartiti))
# Visualizza il DataFrame modificato
#print(len(listapartiti.drop_duplicates()))
#listapartiti = [partito.replace("'", "").replace('"', '') for partito in listapartiti]

#comuni = list(set(listapartiti) & set(df['A']))
#non_comuni = list(set(listapartiti) - set(df['A']))
#print("valori non comuni")
#print(len(non_comuni))
#print(non_comuni)
#print("valori comuni")
#print(comuni)
#print("la lista")
print(len(listapartiti))
print(listapartiti)
# Crea un nuovo DataFrame con la colonna dei partiti modificati
#df_modificato = pd.DataFrame({'Partito Modificato': nuova_lista_partiti})
#part = df_modificato["Partito Modificato"].tolist()
#print(nuova_lista_partiti)
#print(len(nuova_lista_partiti))
#print(part)
# Visualizza il DataFrame risultante
#print(df_modificato)
# Leggi il file Excel con la mappatura dei partiti
#print(df_partito_totale)


import pandas as pd
from wikidataintegrator import wdi_core
import requests
import json
from urllib.parse import quote
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
from ratelimit import limits, sleep_and_retry
import re 
from requests.exceptions import RequestException, TooManyRedirects
@sleep_and_retry
@limits(calls=1, period=2)
def run_sparql_query(query):
    url = 'https://query.wikidata.org/sparql'
    params = {'format': 'json', 'query': query}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if 'results' in data and 'bindings' in data['results']:
            return data['results']['bindings']
        else:
            print('Risposta JSON non valida: dati mancanti.')
    except (RequestException, TooManyRedirects) as e:
        print(f'Errore nella richiesta HTTP: {str(e)}')
    except json.JSONDecodeError as e:
        print(f'Errore nella decodifica della risposta JSON: {str(e)}')

query_template = '''
SELECT DISTINCT ?party ?partyLabel ?al WHERE {{
  ?party wdt:P31 wd:Q7278;
         rdfs:label ?partyLabel;
         wdt:P17 wd:Q38;
         OPTIONAL {{ ?party wdt:P1387 ?alignment. }}
  OPTIONAL {{ ?alignment rdfs:label ?al. }}
  FILTER(LANG(?partyLabel) = "it")
  FILTER(LANG(?al) = "it")
  FILTER(CONTAINS(LCASE(?partyLabel), LCASE("{search_term}")))
}}
'''

processed_parties = set()  # set per tenere traccia dei partiti elaborati
results_list = []  # lista per salvare i risultati dei partiti e allineamenti

for partito in listapartiti:
    query = query_template.format(search_term=partito)
    results = run_sparql_query(query)
    
    print(f"Risultati per il partito: {partito}")
    
    if results:
        for result in results:
            party_label = result['partyLabel']['value']
            
            # Verifica se il partito è già stato elaborato
            if party_label in processed_parties:
                continue
            
            processed_parties.add(party_label)  # Aggiungi il partito al set
            
            alignment_label = result.get('al', {}).get('value', '')  # Estrai l'allineamento politico
            
            print(f"Partito: {party_label}")
            print(f"Allineamento: {alignment_label}")
            
            results_list.append({'Partito': party_label, 'Allineamento Politico': alignment_label})
    else:
        print("Nessun risultato trovato")
        
    print()

# Creazione del dataframe
df = pd.DataFrame(results_list)
partiti_trovati = [result['Partito'] for result in results_list]

# Lista dei partiti di cui non si è trovata l'informazione
partiti_non_trovati = [partito for partito in listapartiti if partito not in partiti_trovati]


# Creazione del dataframe
df_non_trovati = pd.DataFrame({'Partito': partiti_non_trovati})
print(df_non_trovati)
print(len(df_non_trovati))
print(df)
print(len(df))
partiti_trovati = df['Partito'].tolist()
partiti_non_trovati = [partito for partito in listapartiti if partito not in partiti_trovati]
df_filtered = df[df['Partito'].isin(listapartiti)]
print("df_filtered")
print(df_filtered)



import pandas as pd
from urllib.parse import urlencode
@sleep_and_retry
@limits(calls=1, period=2)
def run_sparql_query(query):
    url = 'https://query.wikidata.org/sparql'
    params = {'format': 'json', 'query': query}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if 'results' in data and 'bindings' in data['results']:
            return data['results']['bindings']
        else:
            print('Risposta JSON non valida: dati mancanti.')
    except (RequestException, TooManyRedirects) as e:
        print(f'Errore nella richiesta HTTP: {str(e)}')
    except json.JSONDecodeError as e:
        print(f'Errore nella decodifica della risposta JSON: {str(e)}')

query_template = '''
SELECT DISTINCT ?party ?partyLabel ?al WHERE {{
  ?party wdt:P31 wd:Q7278;
         rdfs:label ?partyLabel;
         wdt:P17 wd:Q38;
         wdt:P1142 ?politicalideology.
  ?politicalideology rdfs:label ?politicalideologylabel;
                     wdt:P1387 ?alignment.
  ?alignment rdfs:label ?al. 
  FILTER(LANG(?partyLabel) = "it")
  FILTER(LANG(?al) = "it")
  FILTER(CONTAINS(?partyLabel, "{search_term}"))
}}
'''

processed_parties = set()  # set per tenere traccia dei partiti elaborati
results_list = []  # lista per salvare i risultati dei partiti e allineamenti

for partito in df_non_trovati['Partito']:
    
    query = query_template.format(search_term=partito)
    results = run_sparql_query(query)
    
    print(f"Risultati per il partito: {partito}")
    
    if results:
        for result in results:
            party_label = result['partyLabel']['value']
            
            # Verifica se il partito è già stato elaborato
            if party_label in processed_parties:
                continue
            
            processed_parties.add(party_label)  # Aggiungi il partito al set
            
            alignment_label = result.get('al', {}).get('value', '')  # Estrai l'allineamento politico
            
            print(f"Partito: {party_label}")
            print(f"Allineamento: {alignment_label}")
            
            results_list.append({'Partito': party_label, 'Allineamento Politico': alignment_label})
    else:
        print("Nessun risultato trovato")
        
    print()

# Creazione del dataframe con i risultati trovati
df_risultati = pd.DataFrame(results_list)
partiti_trovati = df_risultati['Partito'].tolist()
partiti_non_trovati = [partito for partito in listapartiti if partito not in partiti_trovati]
df_filtered2 = df_risultati[df_risultati['Partito'].isin(listapartiti)]
print(df_risultati)

