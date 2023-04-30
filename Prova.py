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


from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe



 


endpoint = "https://dati.camera.it/sparql"

#QUERY TUTTE LE DONNE

querydonne = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatura. 
 } ORDER BY ?legislatura
     
"""

dffemale = sparql_dataframe.get(endpoint, querydonne)

"""SELECT DISTINCT ?nome ?label where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatura.
  ?legislatura rdfs:label ?labelnome. 
  ?legislatura dc:title ?label. 
 } ORDER BY ?legislatura """




#QUERY LUOGHI NASCITA 

querycittànascita = """select ?luogoNascital {
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?luogoNascital.

        } 
     """
dfcittànascita = sparql_dataframe.get(endpoint, querycittànascita)

queryregioninascita = """
    select ?regione ?persona{
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri ocd:parentADM3 ?regione . 
    
    } """ 


dfregioninascita = sparql_dataframe.get(endpoint, queryregioninascita)



#QUERY GRUPPO PARLAMENTARE DONNE 
querygruppopardonne = """SELECT DISTINCT ?nome ?gruppoPar where {
  
  ?nome foaf:gender "female".
  ?nome ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.
  
 } """

dfgruppopardonne = sparql_dataframe.get(endpoint, querygruppopardonne)

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



#QUERY PRESIDENTESSA DEL CONSIGLIO

querypresidentessaconsiglio = """
SELECT DISTINCT ?nome ?cognome where {
  ?legislatura ocd:rif_governo ?governo. 
  ?governo ocd:rif_presidenteConsiglioMinistri ?presidente. 
  ?presidente dc:title ?label. 
   ?presidente ocd:rif_persona ?persona. 
   ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona foaf:gender "female". 
  
   
 } """
dfpresidentessaconsiglio = sparql_dataframe.get(endpoint, querypresidentessaconsiglio)

#QUERY CONTO NUMERO PRESIDENTESSE CONSIGLIO 

querynumerocontopresidentesseconsiglio = """SELECT (COUNT(*) AS ?NUMERO)
WHERE {
  { SELECT DISTINCT ?nome ?cognome WHERE {
   
  ?legislatura ocd:rif_governo ?governo. 
  ?governo ocd:rif_presidenteConsiglioMinistri ?presidente. 
  ?presidente dc:title ?label. 
   ?presidente ocd:rif_persona ?persona. 
   ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona foaf:gender "female". } }} """

dfnumeropresidentesse = sparql_dataframe.get(endpoint, querynumerocontopresidentesseconsiglio)










print(dfpresidentessaconsiglio) 
