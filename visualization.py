from geopy.geocoders import Nominatim
import folium
import pandas as pd
from IPython.display import IFrame

df = pd.read_csv('femalecities.csv')
# calling the Nominatim tool


loc = Nominatim(user_agent="GetLoc")
lat=[]
long=[]
region=[]
# entering the location name
for index, row in df.iterrows():
    getLoc = loc.geocode(row[0])
    if getLoc is not None:
        lat.append(getLoc.latitude)
        long.append(getLoc.longitude)
        region.append(row[1]) # aggiungi la regione di appartenenza della città
df_cord = pd.concat([pd.Series(lat, name='Lat'), pd.Series(long, name='Long'), pd.Series(region, name='Region')], axis=1)
m = folium.Map(df_cord[['Lat', 'Long']].mean().values.tolist())

# definisci un dizionario di icone, una per ogni regione
icon_dict = {
    'Nord': 'cloud',
    'Centro': 'star',
    'Sud': 'heart'
}

for lat, lon, reg in zip(df_cord['Lat'], df_cord['Long'], df_cord['Region']):
    # utilizza la funzione icon di Folium per specificare l'icona da utilizzare per ogni marker
    folium.Marker(location=[lat, lon], icon=folium.Icon(icon=icon_dict[reg])).add_to(m)

sw = df_cord[['Lat', 'Long']].min().values.tolist()
ne = df_cord[['Lat', 'Long']].max().values.tolist()

m.fit_bounds([sw, ne])
map2_html = m._repr_html_()
m.save('map2.html')
IFrame(src='map2.html', width='100%', height='500px')


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
