 # importing geopy library
from geopy.geocoders import Nominatim
import folium
import pandas as pd
from IPython.display import IFrame

df = pd.read_csv('femalecities.csv')
# calling the Nominatim tool
loc = Nominatim(user_agent="GetLoc", timeout=5)
lat=[]
long=[]
# entering the location name
for elem, name in df['città'].iteritems():
    getLoc = loc.geocode(elem)
    if getLoc is not None:
        lat.append(getLoc.latitude)
        long.append(getLoc.longitude)
df_cord = pd.concat([pd.Series(lat, name='Lat'), pd.Series(long, name='Long')], axis=1)
m = folium.Map(df_cord[['Lat', 'Long']].mean().values.tolist())

for lat, lon in zip(df_cord['Lat'], df_cord['Long']):
    folium.Marker([lat, lon]).add_to(m)

sw = df_cord[['Lat', 'Long']].min().values.tolist()
ne = df_cord[['Lat', 'Long']].max().values.tolist()

m.fit_bounds([sw, ne])
map_html = m._repr_html_()
IFrame(src=m._repr_html_(), width='100%', height='500px')

for lat, lon in zip(df_cord['Lat'], df_cord['Long']):
    folium.Marker([lat, lon]).add_to(m)




"""
endpoint = "https://dati.camera.it/sparql"

q4 = select ?nome ?cognome ?città?regione where {
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?città.
 OPTIONAL { ?luogoNascitaUri ocd:parentADM3 ?regione .}
}
df = sparql_dataframe.get(endpoint, q4)
dataframecities = df["città"]

# count the frequency of each city and create a new dataframe
city_counts = df['città'].value_counts().reset_index()
city_counts.columns = ['città', 'count']

# write the result to a CSV file
city_counts.to_csv('femalecities.csv', index=False)




#for idx, row in merged.iterrows():
  #  ax.annotate(text=str(row['count']), xy=row['geometry'].centroid.coords[0], fontsize=8)


"""
