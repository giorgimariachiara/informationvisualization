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

SELECT (COUNT(?nome) AS ?totale) where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatural. 
  ?legislatural dc:title ?legislatura. 
 }"""

dfnumerototdonne = sparql_dataframe.get(endpoint, querynumerototdonne)


querynumerototuomini = """

SELECT (COUNT(?nome) AS ?totale) where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatural. 
  ?legislatural dc:title ?legislatura. 
 }"""

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
   select ?regione {
  ?persona foaf:gender "female".
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri ocd:parentADM3 ?regione . } """ 


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


#QUERY TOTALE NUMERO STUDI UOMO 

querytotstudiuomo = """SELECT (sum(?numero)as ?totale) where {
SELECT DISTINCT ?descrizione (COUNT(?descrizione) as ?numero) where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  
 }
group by ?descrizione}"""


dftotstudiuomo = sparql_dataframe.get(endpoint, querytotstudiuomo)

# Import libraries
import pandas as pd
import matplotlib.pyplot as plt
# Import libraries
import geopandas as gpd
import matplotlib.pyplot as plt

prova = """select distinct ?persona ?legislatura {
  ?persona foaf:gender "female".
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg ?legislatura. } ORDER BY ?legislatura"""

#query informazioni donne nelle legislature ma se una ha partecipato in più legislature è presente più volte (numero totale di righe 3150)
#se invece vediamo solo nome e cognome e non anche legislatura le righe diventano 905
#secondo questa qiery per gli uomini sono nel primo caso e poi 
q = """ 
SELECT distinct (COUNT(?name ?cognome) as ?totale) where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?name. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg ?legislatura.
 }"""


#questa query dovrebbe contare calcolando solo nome e cognome (però potrebbero essere omonimi?)
q2 = """SELECT (COUNT(DISTINCT CONCAT(COALESCE(?name, ''), COALESCE(?cognome, ''))) as ?count) where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?name. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg ?legislatura.
 } """
q3 = """

SELECT distinct ?name ?cognome where {
  
  ?nome foaf:gender "female".
  ?nome foaf:firstName ?name.
  ?nome foaf:surname ?cognome. 
  ?nome ocd:rif_leg ?legislatural. 
  ?legislatural dc:title ?legislatura. 
 }"""
provaaa = sparql_dataframe.get(endpoint, q3)


#prova map visualization il totale delle città per le donne è 905 qui sia se metto ?nome ?=cognome che se metto solo ?nome e il luogo mi da sempre 905 

q4 = """select ?nome ?cognome ?città ?regione where {
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?città.
 OPTIONAL { ?luogoNascitaUri ocd:parentADM3 ?regione .}
}"""
df = sparql_dataframe.get(endpoint, q4)

"""
geolocator =  Bing(api_key='Deputiescities')
df['location'] = df['luogoNascital'].apply(geolocator.geocode)
df['point'] = df['location'].apply(lambda loc: tuple(loc.point) if loc else None)

m = folium.Map(location=[45.5231, -122.6765], zoom_start=4)

for index, row in df.iterrows():
    if row['point'] is not None:
        folium.Marker(location=row['point'], popup=row['Name']).add_to(m)

m 
"""
"""
import folium

# Create a map centered on the first city in the DataFrame
m = folium.Map(location=[df.loc[0, 'città'], df.loc[0, 'regione']])

# Loop through each row in the DataFrame
for index, row in df.iterrows():
    # Check if the region is NaN (i.e. missing)
    if pd.isna(row['regione']):
        # If the region is missing, use only the city for the location
        location = [row['città']]
    else:
        # Otherwise, use both the city and region for the location
        location = [row['città'], row['regione']]
    
    # Create a marker for the person's location and add it to the map
    tooltip = f"{row['nome']} {row['cognome']}"
    folium.Marker(location=location, tooltip=tooltip).add_to(m)

# Display the map
m.save('map2.html')
"""
"""

"""
q4 = """select ?nome ?cognome ?città ?regione where {
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?città.
 OPTIONAL { ?luogoNascitaUri ocd:parentADM3 ?regione .}
}"""

q5 ="""select ?nome ?cognome ?città ?regione ?gender where {
  ?persona foaf:gender ?gender. 
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?città.
 OPTIONAL { ?luogoNascitaUri ocd:parentADM3 ?regione .}}"""


q6 ="""select distinct ?nome ?cognome ?nascita ?città where {
  ?persona foaf:gender ?gender. 
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
 OPTIONAL { ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?città.}}"""
df = sparql_dataframe.get(endpoint, q5) 
dataframepermappa = df.drop(columns=['nome', 'cognome'])
dataframepermappa.to_csv('deputies.csv', index = False) 

# importing geopy library
"""
from geopy.geocoders import Nominatim
import folium
# calling the Nominatim tool
loc = Nominatim(user_agent="cities_map")
lat=[]
long=[] 

listacittà = df['città'].tolist() 

# entering the location name
for elem in listacittà: 
    getLoc = loc.geocode(elem) 
    lat.append(getLoc.latitude) 
    long.append(getLoc.longitude) 

city_list=list(zip(lat, long)) 
df_coordinates = pd.DataFrame(columns = ["Lat", "Long"]) 
df_coordinates["Lat"]=lat 
df_coordinates["Long"]=long 

m = folium.Map(df_coordinates[['Lat', 'Long']].mean().values.tolist()) 

for lat, lon in zip(df_coordinates['Lat'], df_coordinates['Long']): 
    folium.Marker([lat, lon]).add_to(m) 

sw = df_coordinates[['Lat', 'Long']].min().values.tolist() 
ne = df_coordinates[['Lat', 'Long']].max().values.tolist() 

m.fit_bounds([sw, ne])  
"""
"""
"""
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

querypertrovareluogonascitawikid = """select distinct ?name ?surname ?nascita ?città where {
  ?persona foaf:gender ?gender. 
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?name. 
  ?persona foaf:surname ?surname. 
 OPTIONAL { ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?città.}} """

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
print(people19)
#hr.to_csv('fileperwiki.csv')
list = list()
for person in people19:
    # get the name and surname from the tuple
    name, surname = person 
    print(person)
    name = name.capitalize()
    surname = surname.capitalize()
    endpoint ="https://query.wikidata.org/sparql"
    # build the SPARQL query string

    query = ('SELECT distinct ?birthplacel WHERE { \
        ?person wdt:P31 wd:Q5. \
        ?person rdfs:label ?personLabel.  \
        ?person rdfs:label "' + name + surname +'". \
       ?person wdt:P19 ?birthplace. \
        ?birthplace wdt:P1705 ?birthplacel.  \
    }')

    dataf = sparql_dataframe.get(endpoint, query)

    list.append(dataf)
    # make the request to the Wikidata SPARQL endpoint
  

"""
#align with wikidata 
import pandas as pd
from wikidataintegrator import wdi_core, wdi_login, wdi_helpers, service_account


# Set up Wikidata login credentials
login = wdi_login.WDLogin(user='ElizaStuglik', pwd='Bologna21@.')


# Load the relevant data from dati.camera.it
df = pd.read_csv('fileperwiki.csv')

# Loop through each row in the dataset and attempt to align it with Wikidata
for index, row in df.iterrows():

    # Define the properties and values to be used for the Wikidata entity
    name = row['name']
    surname = row['surname']
    place_of_birth = None
    


# Define the Wikidata endpoint to use
endpoint = 'https://query.wikidata.org/sparql'

# Search for the entity using the person's name
query = f"SELECT ?item WHERE {{ ?item rdfs:label '{name}'@en }}"
results = wdi_core.WDItemEngine.execute_sparql_query(query, endpoint)

# Check if the search returned any results
if results['bindings']:
    # Get the first entity in the search results
    entity_id = results['bindings'][0]['item']['value'].split('/')[-1]
    
    # Create a new WDItemEngine object for the entity
    wd_entity = wdi_core.WDItemEngine(wd_item_id=entity_id)
    
    # ... rest of your code goes here
else:
    print(f"No matching entities found for {name}")

    # Search for a matching entity on Wikidata using the name
    entity_id = wdi_core.WDItemEngine.get_wd_search_results(name)

    # If a match is found, update the entity with the new information
    if entity_id:
        # Define the Wikidata entity to be updated
        wd_entity = wdi_core.WDItemEngine(wd_item_id=entity_id[0]['id'])

        # Define the properties and values to be added or updated
        pob_prop = wdi_core.WDString(value=place_of_birth, prop_nr='P19', is_qualifier=True)

        # Update the entity with the new properties and values
        wd_entity.update(data=[pob_prop], login=login)
        
    # If no match is found, create a new entity on Wikidata
    else:
        # Define the Wikidata entity to be created
        wd_entity = wdi_core.WDItemEngine(new_item=True, data=[wdi_core.WDString(value=name, prop_nr='P31')])

        # Define the properties and values to be added
        pob_prop = wdi_core.WDString(value=place_of_birth, prop_nr='P19', is_qualifier=True)

        # Add the new properties and values to the entity
        wd_entity.set_label(name)
        wd_entity.set_description('Person')
        wd_entity.set_aliases([name])
        wd_entity.update(data=[pob_prop], login=login)

"""