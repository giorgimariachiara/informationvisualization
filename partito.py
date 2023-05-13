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
import matplotlib.pyplot as plt
import numpy as np

# Generate some sample data
parties = ['Party A', 'Party B', 'Party C', 'Party D']
male_count = [100, 120, 80, 90]
female_count = [110, 130, 70, 80]

# Create a figure with two subplots
fig, ax = plt.subplots(ncols=2, sharey=True, figsize=(10, 6))

# Plot male data in the first subplot
ax[0].barh(np.arange(len(parties)), male_count, height=0.8, color='steelblue')
ax[0].set(title='Male Members', xlabel='Count')
ax[0].set_yticks(np.arange(len(parties)))
ax[0].set_yticklabels(parties)

# Plot female data in the second subplot
ax[1].barh(np.arange(len(parties)), -female_count, height=0.8, color='indianred')
ax[1].set(title='Female Members', xlabel='Count')
ax[1].set_yticks(np.arange(len(parties)))
ax[1].set_yticklabels([])

# Add a vertical line at x=0 to separate male and female data
ax[0].axvline(x=0, color='black', lw=1)
ax[1].axvline(x=0, color='black', lw=1)

# Add legend
fig.legend(['Male', 'Female'], loc='upper right')

# Set tight layout and save the figure
fig.tight_layout()
plt.savefig('population_pyramid.png', dpi=300)
plt.show()
"""
import matplotlib.pyplot as plt

# create sample data
#male_dict = counts_dict_uomo
#female_dict = counts_dict

#female_keys = list(female_dict.keys())
#print(female_keys)


import matplotlib.pyplot as plt
import numpy as np

# sample data
"""
parties = list(male_dict.keys())

# create subplots for each party
fig, axs = plt.subplots(len(parties), 1, figsize=(8, 6*len(parties)), sharex=True)

for i, party in enumerate(parties):
    # create x positions for the bars
    x = np.arange(2)
    
    # get the counts for the party
    male_count = male_dict[party]
    female_count = female_dict[party]
    
    # plot the bars for the party
    axs[i].bar(x[0], male_count, color='b', label='Male')
    axs[i].bar(x[1], female_count, color='r', label='Female')
    
    # set the title and y-axis label for the subplot
    axs[i].set_title(party)
    axs[i].set_ylabel('Count')
    
    # remove x-axis ticks and labels
    axs[i].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)

# add legend to the first subplot
axs[0].legend()

plt.show()
"""
"""
# plot male and female horizontal bar charts
fig, ax = plt.subplots(nrows=2, ncols=1, figsize=(16,18))

ax[0].barh(list(male_dict.keys()), list(male_dict.values()), color='b')
ax[0].set_title("Male Counts")
ax[0].tick_params(axis='y', labelsize=2)

ax[1].barh(list(female_dict.keys()), list(female_dict.values()), color='r')
ax[1].set_title("Female Counts")
ax[1].tick_params(axis='y', labelsize=2)

plt.show()
"""
"""
import matplotlib.pyplot as plt


# create a bar chart of female political parties
import matplotlib.pyplot as plt

# create a horizontal bar chart of female political parties
fig, ax = plt.subplots(figsize=(16, 20))
ax.barh(list(female_dict.keys()), list(female_dict.values()), color='r')

# set the chart title and axis labels
ax.set_title('Female Political Party Counts', fontsize=20)
ax.set_xlabel('Count', fontsize=16)
ax.set_ylabel('Party', fontsize=12)

# rotate the party labels diagonally for better visibility
for tick in ax.get_yticklabels():
    tick.set_rotation(0)

# set the font size for the tick labels
ax.tick_params(axis='both', labelsize=4)
for tick in ax.xaxis.get_major_ticks():
    tick.label.set_fontweight('bold')

# display the chart
plt.show()

matching_keys = set(male_dict.keys()) & set(female_dict.keys())
print("Matching keys:")

# find the non-matching keys
non_matching_keys = set(male_dict.keys()) ^ set(female_dict.keys())
print("Non-matching keys:")
"""

"""
# Create a SPARQLWrapper object
sparql = SPARQLWrapper(endpoint_url)


# Set the query string for the SPARQLWrapper object
sparql.setQuery(queryorientamento)


# Execute the query and get the results
results = sparql.query().convert()

# Print the results
for result in results["results"]["bindings"]:
    print(result["itemLabel"]["value"])
"""