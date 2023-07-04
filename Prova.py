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

queryincaricodonneee = """SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?ufficio 
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

queryincaricodeputatedonne = """ SELECT DISTINCT ?d ?legislatura ?cognome ?nome 
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


}    """

df_incarico_donne = get(endpoint, queryincaricodeputatedonne)
df_incarico_donne = df_incarico_donne[["nome", "cognome", "legislatura", "ufficio"]]
df_incarico_donne = df_incarico_donne.rename(columns={'ufficio': 'incarico'})
#df_incarico_donne.to_csv("incaricodonne.csv",  index=False, index_label=False)
#df_incarico_donne = df_incarico_donne.drop_duplicates()
#print(len(df_incarico_donne))
#conteggio_female = df_incarico_donne['gender'].value_counts()['female']
#print(conteggio_female)
#print(df_incarico_donne)

queryincaricodeputatiuomini = """
SELECT DISTINCT ?d ?legislatura ?cognome ?nome 
  ?ufficio
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "male" ;foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.


}   """

df_incarico_uomini = get(endpoint, queryincaricodeputatiuomini)
df_incarico_uomini = df_incarico_uomini[["nome", "cognome", "legislatura", "ufficio"]]
df_incarico_uomini = df_incarico_uomini.rename(columns={'ufficio': 'incarico'})
df_incarico_uomini.to_csv("incaricouomini.csv",  index=False, index_label=False)
print(len(df_incarico_donne))
print(len(df_incarico_uomini))
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
df_male = df_incarico_uomini[df_incarico_uomini['gender'] == 'male']
#print(len(df_male))
#print(len(df_female))
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

import pandas as pd
import requests

def getdatafromwiki(parties):
    for party in parties:
        query = '''
        SELECT distinct ?party ?partyLabel ?alignment ?al WHERE {
          ?party wdt:P31 wd:Q7278;
                 rdfs:label ?partyLabel;
                 wdt:P17 wd:Q38;
                 wdt:P1387 ?alignment.
          ?alignment rdfs:label ?al. 
          FILTER(LANG(?partyLabel) = "it" && CONTAINS(LCASE(?partyLabel), "''' + party.lower() + '''")).
          FILTER(LANG(?al) = "it")
        }
        '''
        url = 'https://query.wikidata.org/sparql'
        r = requests.get(url, params={'format': 'json', 'query': query})
        data = r.json()
        bindings = data['results']['bindings']
        politicalalignment = [binding['al']['value'] for binding in bindings]
        print(f"Partito: {party}")
        print(f"Political alignment: {', '.join(politicalalignment)}")
        print()

# Esempio di utilizzo
parties = ['Partito socialista italiano', 'Movimento 5 stelle']
getdatafromwiki(parties)

