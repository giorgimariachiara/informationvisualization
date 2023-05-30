import csv
from logging import raiseExceptions
import pandas as pd
from json import load
from pandas import DataFrame
import os.path
from sqlite3 import connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import concat, read_sql
import csv
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import sys
import time
import chart_studio
chart_studio.tools.set_credentials_file(username='DemoAccount', api_key='lr1c37zw')
pd.set_option('display.max_rows', None)

from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe
from sparql_dataframe import get

endpoint = "https://dati.camera.it/sparql"

#QUERY CARICA DONNE 
querycaricadonne = """SELECT DISTINCT ?nome ?cognome ?ufficio ?organo where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome.
  ?persona foaf:surname ?cognome. 
  ?persona ocd:rif_ufficioParlamentare ?ufficioUri.
  ?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.
  ?organoUri dc:title ?organo.
  
 } """

dfcaricadonne = sparql_dataframe.get(endpoint, querycaricadonne)

#QUERY PRESIDENTI DEL CONSIGLIO

querypresidenticonsiglio = """
SELECT DISTINCT ?nome ?cognome ?persona WHERE {
  ?legislatura ocd:rif_governo ?governo.
  ?governo ocd:rif_presidenteConsiglioMinistri ?presidente.
  ?presidente dc:title ?label.
  ?presidente ocd:startDate ?startDate.
  FILTER (xsd:dateTime(?startDate) >= xsd:dateTime("1946-07-13T00:00:00Z"))
  ?presidente ocd:rif_persona ?persona.
  ?persona foaf:firstName ?nome.
  ?persona foaf:surname ?cognome.
  ?persona foaf:gender "male".
}"""
df_presidenti_consiglio = sparql_dataframe.get(endpoint, querypresidenticonsiglio)
df_presidenti_consiglio = df_presidenti_consiglio.drop_duplicates(subset=['nome', 'cognome'])
df_presidenti_consiglio['nome'] = df_presidenti_consiglio['nome'] + ' ' + df_presidenti_consiglio['cognome']
df_presidenti_consiglio = df_presidenti_consiglio[["nome"]]
df_presidenti_consiglio = df_presidenti_consiglio.assign(gender="male")

#QUERY PRESIDENTESSE DEL CONSIGLIO

querypresidentesseconsiglio = """
SELECT DISTINCT ?nome ?cognome ?persona WHERE {
  ?legislatura ocd:rif_governo ?governo.
  ?governo ocd:rif_presidenteConsiglioMinistri ?presidente.
  ?presidente dc:title ?label.
  ?presidente ocd:startDate ?startDate.
  FILTER (xsd:dateTime(?startDate) >= xsd:dateTime("1946-07-13T00:00:00Z"))
  ?presidente ocd:rif_persona ?persona.
  ?persona foaf:firstName ?nome.
  ?persona foaf:surname ?cognome.
  ?persona foaf:gender "female".
}"""
df_presidentesse_consiglio = sparql_dataframe.get(endpoint, querypresidentesseconsiglio)
df_presidentesse_consiglio = df_presidentesse_consiglio.drop_duplicates(subset=['nome', 'cognome'])
df_presidentesse_consiglio['nome'] = df_presidentesse_consiglio['nome'] + ' ' + df_presidentesse_consiglio['cognome']
df_presidentesse_consiglio = df_presidentesse_consiglio[["nome"]]
df_presidentesse_consiglio = df_presidentesse_consiglio.assign(gender="female")

df_presidenti_consiglio_totale = pd.concat([df_presidenti_consiglio, df_presidentesse_consiglio])
#df_presidenti_consiglio_totale.to_csv("presidenticonsigliototale.csv",  index=False, index_label=False)

#print(df_presidenti_consiglio_totale)

"""
import pandas as pd
import matplotlib.pyplot as plt

# Carica il file CSV
df = pd.read_csv('presidenticonsigliototale.csv')

# Calcola il numero di donne e uomini ministri
conteggio_generi = df['gender'].value_counts()

# Crea la pie chart
labels = conteggio_generi.index
sizes = conteggio_generi.values
colors = ['skyblue', 'lightcoral']

plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%')
plt.axis('equal')  # Rende il grafico un cerchio
plt.title('Distribuzione dei ministri per genere')
plt.show()

"""


"""
import requests 

hr = sparql_dataframe.get(endpoint, querypertrovareluogonascitawikid)

hr = hr[hr.isnull().any(axis=1)]
people = list(zip(hr['name'], hr['surname']))
people1 = people[:len(people)//2]
people3 = people1[:len(people1)//2]
people5= people1[len(people1)//2:]
people7 =people3[:len(people3)//2]
people9 = people3[len(people3)//2:]
people11 = people7[:len(people7)//2]
people13= people11[:len(people11)//2]
people15 = people13[:len(people13)//2]
people17 = people15[:len(people15)//2]
people19 = people17[:len(people17)//2]
people2 = people[len(people)//2:]
#print(people19)
#hr.to_csv('fileperwiki.csv')
def getdata(list):
   import pandas as pd
from SPARQLWrapper import SPARQLWrapper, TSV

for name_tuple in people19:
        # Get the name and surname from the tuple
        first, last = name_tuple
        name_str = f"{first} {last}"
        print(name_str)
def getdatafromwiki(name_list):
    endpoint = "https://query.wikidata.org/sparql"
    dfs = []
    for name_tuple in name_list:
        # Get the name and surname from the tuple
        first, last = name_tuple
        name_str = f"{first} {last}"

        # Build the SPARQL query string
        
        query = ('SELECT DISTINCT ?birthplacel WHERE { \
            ?person wdt:P31 wd:Q5. \
            ?person rdfs:label ?personLabel. \
            ?person rdfs:label "' + name_str +'". \
            ?person wdt:P19 ?birthplace. \
            ?birthplace wdt:P1705 ?birthplacel. \
        }')
        
        # Make the request to the Wikidata SPARQL endpoint
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(TSV)
        data = sparql.query().convert()
        
        # Convert the TSV data to a Pandas DataFrame
        df = pd.read_csv(data.splitlines(), sep='\t', header=None, names=['birthplace'])
        
        # Append the DataFrame to the list of results
        dfs.append(df)
    
    return dfs

def getdatafromwiki(list):
  for person in list:
      name_str_list = [f"{first} {last}" for first, last in list]
      for el in name_str_list:
      # get the name and surname from the tuple
        # build the SPARQL query string

        query = ('SELECT distinct ?birthplacel WHERE { \
            ?person wdt:P31 wd:Q5. \
            ?person rdfs:label ?personLabel.  \
            ?person rdfs:label "' + el +'". \
          ?person wdt:P19 ?birthplace. \
            ?birthplace wdt:P1705 ?birthplacel.  \
        }')
        endpoint ="https://query.wikidata.org/sparql"
        dataf = sparql_dataframe.get(endpoint, query)
        print(dataf)
        #lista.append(dataf)
        #make the request to the Wikidata SPARQL endpoint
    
print(getdata(people19))


SELECT DISTINCT ?person ?labelNome ?labelCognome WHERE {
  ?person wdt:P31 wd:Q5 .
  ?person wdt:P735 ?nome. 
  ?person wdt:P734 ?cognome.
  ?nome rdfs:label ?labelNome . 
  ?cognome rdfs:label ?labelCognome . 
   FILTER (STRENDS(?labelNome, "Dino"))
  FILTER (STRENDS(?labelCognome, "Secco"))
} 
"""
queryincaricodonne = """SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?ufficio 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female";foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri. 
?luogoNascitaUri dc:title ?luogoNascita. 
}
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.


} """

qqq = """ SELECT DISTINCT ?d ?persona ?cognome ?nome ?gender ?luogoNascita ?ufficio 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "male" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri. 
?luogoNascitaUri dc:title ?luogoNascita. 
}
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.


}   """

querycontocarichedonna = """ SELECT DISTINCT ?inca ?d ?nome
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri. 
?luogoNascitaUri dc:title ?luogoNascita. 
}
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?inca.


}  """

df_incarico_donne = get(endpoint, qqq)
#df_incarico_donne = df_incarico_donne.drop_duplicates()
#print(len(df_incarico_donne))
#conteggio_female = df_incarico_donne['gender'].value_counts()['female']
#print(conteggio_female)
#print(df_incarico_donne)
""" SELECT DISTINCT ?ruolo (COUNT(DISTINCT ?persona) as ?numeroPersone)
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "male" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri. 
?luogoNascitaUri dc:title ?luogoNascita. 
}
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
  ?d ocd:rif_incarico ?incarico.
  ?incarico ocd:ruolo ?ruolo. 


} group by ?ruolo  """

queryincaricodeputatedonne = """
SELECT ?d ?cognome ?nome 
  ?ufficio
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.


}   """
df_incarico_donne = get(endpoint, queryincaricodeputatedonne)
df_incarico_donne = df_incarico_donne.drop_duplicates()
#print(len(df_incarico_donne))
queryincaricodeputatiuomini = """
SELECT ?d ?cognome ?nome ?gender ?legislatura
  ?ufficio
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender ?gender ;foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.
?d ocd:rif_incarico ?incarico.
  ?incarico ocd:ruolo ?ruolo. 


}   """
df_incarico_uomini = get(endpoint, queryincaricodeputatiuomini)
df_incarico_uomini = df_incarico_uomini.drop_duplicates()
#df_female = df_incarico_totale[df_incarico_totale['genere'] == 'female']
#print(len(df_incarico_uomini))
 #print(df_incarico_uomini)
# Filtra le righe con genere "male"
df_male = df_incarico_uomini[df_incarico_uomini['gender'] == 'female']
print(len(df_male))
print(df_male)
#print(df_male)
#print(df_female)
#print(df_male)
#print(len(df_female))
#print(len(df_male))
#df_incarico_finale = pd.concat([df_female, df_male])
#print(len(df_incarico_totale))
#print(df_incarico_totale)
#df_incarico_totale.to_csv("incaricodeputati.csv",  index=False, index_label=False)

#df_incarico_ = df_incarico_.drop_duplicates(['nome', 'cognome', 'ufficio']) 
#conteggio_f = df_incarico_['genere'].value_counts()['female']
#conteggio_m = df_incarico_['cognome'].value_counts()['CIRIELLI']
#print(df_incarico_)
#print(conteggio_f)
#print(conteggio_m)
#print(df_incarico_)
