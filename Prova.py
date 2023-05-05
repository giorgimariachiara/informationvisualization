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


from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe



 


endpoint = "https://dati.camera.it/sparql"

#QUERY TUTTE LE DONNE

querydonne = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?legislatural dc:title ?legislatura. 
 } ORDER BY ?legislatural
     
"""

dffemale = sparql_dataframe.get(endpoint, querydonne)

#QUERY NUMERO TOTALE DONNE 
querynumerototdonne = """
SELECT (SUM(?totale) AS ?tot) WHERE {
SELECT (COUNT(?nome) AS ?totale) where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?legislatural dc:title ?legislatura. 
 }}"""

dfnumerototdonne = sparql_dataframe.get(endpoint, querynumerototdonne)


querynumerototuomini = """
SELECT (SUM(?totale) AS ?tot) WHERE {
SELECT (COUNT(?nome) AS ?totale) where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatural. 
  ?legislatural dc:title ?legislatura. 
 }}"""

dfnumerototuomini = sparql_dataframe.get(endpoint, querynumerototuomini)

#QUERY LUOGHI NASCITA 

querycittànascita = """select ?luogoNascital {
  ?persona foaf:gender "female".
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


#print(dftotstudidonne) 

"""
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

app = Dash(__name__)


app.layout = html.Div([
    html.H4('Interactive color selection with simple Dash example'),
    html.P("Select color:"),
    dcc.Dropdown(
        id="dropdown",
        options=['Gold', 'MediumTurquoise', 'LightGreen'],
        value='Gold',
        clearable=False,
    ),
    dcc.Graph(id="graph"),
])


@app.callback(
    Output("graph", "figure"), 
    Input("dropdown", "value"))
def display_color(color):
    fig = go.Figure(
        data=go.Bar(y=[2, 3, 1], # replace with your own data source
                    marker_color=color))
    return fig


app.run_server(debug=True)
"""
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

dftotalenumerodonnelaurea = sparql_dataframe.get(endpoint, querytotalenumerodonnelaurea)

#TOTALE DONNE SENZA LAUREA 
querytotnonlaureadonne = """PREFIX owl: <http://www.w3.org/2002/07/owl#>


SELECT (SUM(?numero) as ?totale) where {
select (COUNT(?descrizione) as ?numero)

where
{?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  

   FILTER regex(?descrizione, "^(?!.*Laurea|laurea)")}
GROUP BY ?descrizione}"""

dftotnumerononlaureadonne = sparql_dataframe.get(endpoint, querytotnonlaureadonne)

queryprovava = """
SELECT ?nome

where
{?nome foaf:gender "female".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>. 
 ?nome dc:description ?descrizione. 
 
} 
"""

dfprovava = sparql_dataframe.get(endpoint, queryprovava)

endpointdbpedia = "https://dbpedia.org/sparql"

queryregionidbpedia = """select ?regione ?point where {{
?regione dbo:type dbr:Regions_of_Italy.
?regione georss:point ?point } UNION {?regione dbo:type dbr:Autonomous_regions_with_special_statute.
?regione georss:point ?point. } 
UNION {?regione dbo:type dbr:Region_of_Italy .
?regione georss:point ?point. }}"""

queryregioniwikidata = """select ?regione ?id where {{
?regione wdt:P31 wd:Q16110.
?regione wdt:P402 ?id } UNION {?regione wdt:P31 wd:Q1710033.
?regione wdt:P402 ?id } 
}"""
dfregionidbpedia = sparql_dataframe.get(endpointdbpedia, queryregionidbpedia)
#print(dfnumerototdonne)


#MARIMEKKO CHART 
# Import libraries
import pandas as pd
import matplotlib.pyplot as plt

# Load the data of Italian deputies and their education level
deputies = pd.read_csv('deputies.csv')

# Group the deputies data by gender and education level
deputies_grouped = deputies.groupby(['gender', 'education']).size().reset_index(name='count')

# Calculate the total number of deputies for each gender
total_males = deputies[deputies['gender'] == 'M']['gender'].count()
total_females = deputies[deputies['gender'] == 'F']['gender'].count()

# Calculate the percentage of deputies for each gender and education level
deputies_grouped['percentage'] = deputies_grouped.apply(lambda row: row['count'] / (total_males if row['gender'] == 'M' else total_females), axis=1)

# Pivot the data to create a Marimekko chart
deputies_pivot = deputies_grouped.pivot(index='gender', columns='education', values='percentage')

# Sort the data by descending order of male percentages
deputies_pivot = deputies_pivot.sort_values(by='M', ascending=False)

# Create a stacked vertical bar chart
fig, ax = plt.subplots(figsize=(10, 8))
ax.bar(deputies_pivot.columns, deputies_pivot.loc['M'], color='b')
ax.bar(deputies_pivot.columns, deputies_pivot.loc['F'], bottom=deputies_pivot.loc['M'], color='r')

# Add labels and title
ax.set_xlabel('Education Level')
ax.set_ylabel('Percentage of Deputies')
ax.set_title('Graduated Deputies in the Italian Chamber of Deputies by Gender')

# Add percentage labels on the bars
for i, v in enumerate(deputies_pivot.loc['M']):
    ax.text(i, v/2, f'{round(v*100, 1)}%', color='white', ha='center', va='center', fontweight='bold')
    ax.text(i, v+deputies_pivot.loc['F'][i]/2, f'{round(deputies_pivot.loc["F"][i]*100, 1)}%', color='white', ha='center', va='center', fontweight='bold')

# Show the plot
plt.show()
