
from SPARQLWrapper import SPARQLWrapper, JSON
import io 
import sparql_dataframe
import matplotlib.pyplot as plt
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
dfgruppoparuomini = sparql_dataframe.get(endpoint, querygruppoparuomini)

dfgruppopardonne = sparql_dataframe.get(endpoint, querygruppopardonne)

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
male_dict = counts_dict_uomo
female_dict = counts_dict

female_keys = list(female_dict.keys())
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

queryorientamento = """SELECT distinct ?partito ?label WHERE {
        ?partito wdt:P31 wd:Q7278.
        ?partito wdt:P17 wd:Q38.
        ?partito rdfs:label ?label. 
  filter (lang(?label) = "it")
          FILTER (str(?label) = "Fronte Verde") 
    }"""
dforientamento = sparql_dataframe.get(endpointwiki, queryorientamento)

endpoint_url = "https://query.wikidata.org/sparql"

# Create a SPARQLWrapper object
sparql = SPARQLWrapper(endpoint_url)


# Set the query string for the SPARQLWrapper object
sparql.setQuery(queryorientamento)

# Execute the query and get the results
results = sparql.query().convert()

# Print the results
for result in results["results"]["bindings"]:
    print(result["itemLabel"]["value"])
