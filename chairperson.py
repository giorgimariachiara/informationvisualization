import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import time
import sparql_dataframe
from sparql_dataframe import get
import json

pd.set_option('display.max_rows', None)

endpoint = "https://dati.camera.it/sparql"


#QUERY PER TROVARE PARTITO 
query_partito_uomini1 = """SELECT DISTINCT ?persona ?cognome ?nome ?dataNascita ?gruppoPar
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

query_partito_uomini2 = """SELECT DISTINCT ?persona ?cognome ?nome ?dataNascita ?gruppoPar
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
?dataNascita ?gruppoPar
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


df_partito_donne = get(endpoint, query_partito_donne)
df_partito_donne['gruppoPar'] = df_partito_donne['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_donne = df_partito_donne.drop_duplicates(["persona","nome", "cognome", "dataNascita", "gruppoPar"])
df_partito_donne = df_partito_donne[["gruppoPar"]]
df_partito_donne.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_donne = df_partito_donne.assign(gender='female')


df_partito_uomini1 = get(endpoint, query_partito_uomini1)
df_partito_uomini1['gruppoPar'] = df_partito_uomini1['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_uomini1 = df_partito_uomini1.drop_duplicates(["persona","nome", "cognome", "dataNascita", "gruppoPar"])
df_partito_uomini1 = df_partito_uomini1[["gruppoPar"]]
df_partito_uomini1.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_uomini1 = df_partito_uomini1.assign(gender='male')

df_partito_uomini2 = get(endpoint, query_partito_uomini2)
df_partito_uomini2['gruppoPar'] = df_partito_uomini2['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_uomini2 = df_partito_uomini2.drop_duplicates(["persona","nome", "cognome", "dataNascita", "gruppoPar"])
df_partito_uomini2 = df_partito_uomini2[["gruppoPar"]]
df_partito_uomini2.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_uomini2 = df_partito_uomini2.assign(gender='male')

df_partito_totale = pd.concat([df_partito_uomini1, df_partito_uomini2, df_partito_donne])

df_partito_totale_no_duplicati = df_partito_totale[["partito"]].drop_duplicates()

listapartiti = df_partito_totale_no_duplicati["partito"].tolist()

# Leggi il file Excel e crea il DataFrame
df_excel = pd.read_excel('Partiti.xlsx')
listapartiti = [partito.lower().strip() for partito in listapartiti]
df_excel['A'] = df_excel['A'].str.lower().str.strip()

# Crea un dizionario dalla colonna A alla colonna B del DataFrame
mappa_partiti = dict(zip(df_excel['A'], df_excel['B']))

# Itera sulla lista dei partiti
for i in range(len(listapartiti)):
    partito = listapartiti[i]
    if partito in mappa_partiti:
        listapartiti[i] = mappa_partiti[partito]
listapartiti = list(set(listapartiti))

query_template = """

SELECT DISTINCT ?chairpersonLabel WHERE {{
  ?party wdt:P31 wd:Q7278;
         wdt:P17 wd:Q38;
     rdfs:label ?partyLabel;
  wdt:P488 ?chairperson.
 ?chairperson rdfs:label ?chairpersonLabel.
  FILTER(LANG(?chairpersonLabel) = "it")
  FILTER(LANG(?partyLabel) = "it")
  FILTER(CONTAINS(?partyLabel, "{search_term}"))
}}
"""

# Inizializzazione della SPARQL endpoint di Wikidata
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

# Creazione delle liste dei partiti con informazioni e senza informazioni
partiti_con_informazioni = []
partiti_senza_informazioni = []

# Esecuzione della query per ogni partito
for partito in listapartiti:
    query = query_template.format(search_term=partito)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    # Elaborazione dei risultati
    if results["results"]["bindings"]:
        rows = [{"Partito": partito, "Chairperson": result["chairpersonLabel"]["value"]} for result in results["results"]["bindings"]]
        partiti_con_informazioni.extend(rows)
    else:
        partiti_senza_informazioni.append(partito)

# Creazione del DataFrame per i partiti con informazioni
df_con_informazioni = pd.DataFrame(partiti_con_informazioni)

# Stampa del DataFrame e della lista
print("Partiti con informazioni:")
print(df_con_informazioni)

print("\nPartiti senza informazioni:")
print(partiti_senza_informazioni)