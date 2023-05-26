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
# Import libraries
import pandas as pd
import matplotlib.pyplot as plt
# Import libraries
import geopandas as gpd
import matplotlib.pyplot as plt

from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe
from sparql_dataframe import get
#qui abbiamo la query dell'orientamento dei partiti e la prova di chart per i diversi partiti donna uomo 


endpoint = "https://dati.camera.it/sparql"
endpointwiki =  "https://query.wikidata.org/sparql"

#QUERY GRUPPO PARLAMENTARE DONNE 
querygruppopardonne = """SELECT DISTINCT ?nome ?cognome (CONCAT(STRBEFORE(?gruppoPar, " ("), STRAFTER(?gruppoPar, ")"))) AS ?gruppo
WHERE {
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona ocd:aderisce ?gruppo. 
  ?gruppo rdfs:label ?gruppoPar.
  FILTER regex(?gruppoPar, "\\\\(.*\\\\)")
}
 """
dfgruppopardonne = get(endpoint, querygruppopardonne)
#print(dfgruppopardonne)

querygruppoparuomini = """SELECT DISTINCT ?nome ?cognome (CONCAT(STRBEFORE(?gruppoPar, " ("), STRAFTER(?gruppoPar, ")"))) AS ?gruppo
WHERE {
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona ocd:aderisce ?gruppo. 
  ?gruppo rdfs:label ?gruppoPar.
  FILTER regex(?gruppoPar, "\\\\(.*\\\\)")
}
 """
#per farla su virtuoso "\\(.*\\)" tocca usare questo che su python non viene preso daje tuttaaaa
dfgruppoparuomini = get(endpoint, querygruppoparuomini)

countuomo= len(dfgruppoparuomini[['nome', 'cognome']].drop_duplicates())
countuomo = dfgruppoparuomini['gruppo'].value_counts()

count = len(dfgruppopardonne[['nome', 'cognome']].drop_duplicates())
counts = dfgruppopardonne['gruppo'].value_counts()
#print(countuomo)
counts_dict_uomo = {}
for index, row in dfgruppoparuomini.iterrows():
    gruppo = row["gruppo"]
    if gruppo in counts_dict_uomo:
        counts_dict_uomo[gruppo] += 1
    else:
        counts_dict_uomo[gruppo] = 1
#print(len(counts_dict_uomo))
counts_dict = {}
for index, row in dfgruppopardonne.iterrows():
    gruppo = row["gruppo"]
    if gruppo in counts_dict:
        counts_dict[gruppo] += 1
    else:
        counts_dict[gruppo] = 1
#print(len(counts_dict))

# create sample data
male_dict = counts_dict_uomo
female_dict = counts_dict

female_keys = list(female_dict.keys())
parties = list(male_dict.keys())
#print(len(parties))
maleparties1= parties[len(parties)//2:]
maleparties2 =parties[:len(parties)//2]
maleparties3 = maleparties1[len(maleparties1)//2:]
maleparties5= maleparties2[:len(maleparties2)//2]
maleparties7 = maleparties3[len(maleparties3)//2:]
maleparties9= maleparties5[:len(maleparties5)]
#print(maleparties1)
#print(maleparties2)
#print(parties.capitalize())


queryorientamento = """SELECT distinct ?partito ?label ?orientamentoLabel  WHERE {{
        ?partito wdt:P31 wd:Q7278.
        ?partito wdt:P17 wd:Q38.
        ?partito wdt:P1142 ?orientamento. 
        ?orientamento rdfs:label ?orientamentoLabel. 
        ?partito rdfs:label ?label. 
  filter (lang(?label) = "it")
  filter (lang(?orientamentoLabel) = "it")
          FILTER contains(str(?label), "{}")
    }
UNION 
{?partito wdt:P31 wd:Q6138528.
        ?partito wdt:P17 wd:Q38.
 ?partito wdt:P1142 ?orientamento. 
 ?orientamento rdfs:label ?orientamentoLabel.
        ?partito rdfs:label ?label. 
  filter (lang(?orientamentoLabel) = "it")
  filter (lang(?label) = "it")
          FILTER contains(str(?label), "{}") }
}"""

"""
pd.set_option('display.max_rows', None)
labels = [party.title() for party in maleparties9]

# initialize an empty list to store the results
output = []

# iterate over the labels and execute the query for each one
for label in labels:
    # replace the placeholder in the query with the label
    query = queryorientamento.replace("{}", label)

    # create a new SPARQL endpoint
    endpoint = SPARQLWrapper(endpointwiki)
    endpoint.setQuery(query)
    endpoint.setReturnFormat(JSON)

    # execute the query and convert the results to a list of dictionaries
    results = endpoint.query().convert()
    bindings = results["results"]["bindings"]
    for b in bindings:
        row = {}
        for k, v in b.items():
            row[k] = v["value"]
        output.append(row)

# create a DataFrame from the output list
df = pd.DataFrame(output)
# print the results
print(df)
"""