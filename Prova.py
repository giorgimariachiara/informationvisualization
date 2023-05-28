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



#QUERY PRESIDENTESSA DEL CONSIGLIO

querypresidentessaconsiglio = """
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
dfpresidentessaconsiglio = sparql_dataframe.get(endpoint, querypresidentessaconsiglio)
dfpresidentessaconsiglio = dfpresidentessaconsiglio.drop_duplicates(subset=['nome', 'cognome'])
print(dfpresidentessaconsiglio)
print(len(dfpresidentessaconsiglio))

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
  ?persona foaf:gender "male". } }} """

dfnumeropresidentesse = sparql_dataframe.get(endpoint, querynumerocontopresidentesseconsiglio)

queryprova = """SELECT ?nome ?cognome ?luogonascita  where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome.
  ?persona foaf:surname ?cognome. 

  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita. 
  ?nascita rdfs:label ?luogonascita. 
  ?nascita ocd:rif_luogo ?luogo. 
 } 
"""
dfprova = sparql_dataframe.get(endpoint, queryprova)

#QUERY STUDI DONNE 

querystudidonne = """prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>

SELECT DISTINCT ?descrizione (COUNT(?descrizione) as ?numero) where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  
 }
group by ?descrizione
"""
dfstudidonne = sparql_dataframe.get(endpoint, querystudidonne)

#QUERY TOTALE NUMERO STUDI DONNE 
querytotstudidonne = """SELECT (sum(?numero)as ?totale) where {
SELECT DISTINCT ?descrizione (COUNT(?descrizione) as ?numero) where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  
 }
group by ?descrizione}
"""

dftotstudidonne = sparql_dataframe.get(endpoint, querytotstudidonne)


#TOTALE DONNE CON LAUREA 

querytotalenumerodonnelaurea ="""PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT (SUM(?numero) as ?totale) where {
select (COUNT(?descrizione) as ?numero)

where
{?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  

   FILTER regex(?descrizione, "^(Laurea|laurea)")}}
"""

"""
SELECT (COUNT(DISTINCT CONCAT(COALESCE(?name, ''), COALESCE(?cognome, ''))) as ?count) where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?name. 
  ?persona foaf:surname ?cognome . 
  ?persona dc:description ?descrizione. 
  FILTER regex(?descrizione, "^(Laurea|laurea)")
 
 }"""
dftotalenumerodonnelaurea = sparql_dataframe.get(endpoint, querytotalenumerodonnelaurea)

#TOTALE DONNE SENZA LAUREA 
querytotnonlaureadonne = """PREFIX owl: <http://www.w3.org/2002/07/owl#>


SELECT (SUM(?numero) as ?totale) where {
select (COUNT(?descrizione) as ?numero)

where
{?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  

   
   
   }
GROUP BY ?descrizione}"""

dftotnumerononlaureadonne = sparql_dataframe.get(endpoint, querytotnonlaureadonne)

#QUERY TOTALE NUMERO STUDI UOMO 

querytotstudiuomo = """SELECT (sum(?numero)as ?totale) where {
SELECT DISTINCT ?descrizione (COUNT(?descrizione) as ?numero) where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  
 }
group by ?descrizione}"""

dftotstudiuomo = sparql_dataframe.get(endpoint, querytotstudiuomo)


q6 ="""select distinct ?nome ?cognome ?nascita ?città where {
  ?persona foaf:gender ?gender. 
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
 OPTIONAL { ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?città.}}"""

"""
# Import libraries
import pandas as pd
import matplotlib.pyplot as plt

# Load the data of graduated male deputies
male_deputies = pd.read_csv('male_deputies.csv')
male_deputies = male_deputies.groupby(['education'])['count'].sum().reset_index()

# Load the data of graduated female deputies
female_deputies = pd.read_csv('female_deputies.csv')
female_deputies = female_deputies.groupby(['education'])['count'].sum().reset_index()

# Merge the male and female dataframes based on the education level
deputies = pd.merge(male_deputies, female_deputies, on='education', suffixes=('_male', '_female'))

# Calculate the total number of deputies for each gender
total_males = male_deputies['count'].sum()
total_females = female_deputies['count'].sum()

# Calculate the percentage of deputies for each gender and education level
deputies['percentage_male'] = deputies['count_male'] / total_males
deputies['percentage_female'] = deputies['count_female'] / total_females

# Create a stacked vertical bar chart
fig, ax = plt.subplots(figsize=(10, 8))
ax.bar(deputies['education'], deputies['percentage_male'], color='b')
ax.bar(deputies['education'], deputies['percentage_female'], bottom=deputies['percentage_male'], color='r')

# Add labels and title
ax.set_xlabel('Education Level')
ax.set_ylabel('Percentage of Deputies')
ax.set_title('Graduated Deputies in the Italian Chamber of Deputies by Gender')

# Add percentage labels on the bars
for i, v in enumerate(deputies['percentage_male']):
    ax.text(i, v/2, f'{round(v*100, 1)}%', color='white', ha='center', va='center', fontweight='bold')
    ax.text(i, v+deputies['percentage_female'][i]/2, f'{round(deputies["percentage_female"][i]*100, 1)}%', color='white', ha='center', va='center', fontweight='bold')

# Show the plot
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
