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
# importing geopy library
from geopy.geocoders import Nominatim
import folium
# calling the Nominatim tool
loc = Nominatim(user_agent="location")
lat=[]
long=[]

listacittà = df['città'].tolist()

# entering the location name
for elem in listacittà:
    getLoc = loc.geocode(elem)
    lat.append(getLoc.latitude)
    long.append(getLoc.longitude)

city_list=list(zip(lat, long))
df_cord = pd.DataFrame(columns = ["Lat", "Long"])
df_cord["Lat"]=lat
df_cord["Long"]=long

m = folium.Map(df_cord[['Lat', 'Long']].mean().values.tolist())

for lat, lon in zip(df_cord['Lat'], df_cord['Long']):
    folium.Marker([lat, lon]).add_to(m)

sw = df_cord[['Lat', 'Long']].min().values.tolist()
ne = df_cord[['Lat', 'Long']].max().values.tolist()

m.fit_bounds([sw, ne])
print(m)