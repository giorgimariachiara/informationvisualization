# importing geopy library
from geopy.geocoders import Nominatim
import folium
import pandas as pd
from IPython.display import IFrame

# Read the CSV file
df = pd.read_csv('deputies.csv')

# Create a dictionary to map region to icon color
icon_dict = {'Nord': 'blue', 'Centro': 'green', 'Sud': 'red'}

# Calculate the city counts
city_counts_dict = df['città'].value_counts().to_dict()

# Calling the Nominatim tool
loc = Nominatim(user_agent="GetLoc")

# Create empty lists to store latitude, longitude, and region
lat = []
lon = []
regione = []

# Iterate over the DataFrame to get location coordinates and region
for index, row in df.iterrows():
    getLoc = loc.geocode(row['città'])
    if getLoc is not None:
        lat.append(getLoc.latitude)
        lon.append(getLoc.longitude)
        regione.append(row['regione'])
    
# Create a DataFrame to store the coordinates and region
df_cord = pd.concat([pd.Series(lat, name='lat'), pd.Series(lon, name='lon'), pd.Series(regione, name='regione')], axis=1)

print(type(df))
"""
# Create a folium map
m = folium.Map(df_cord[['lat', 'lon']].mean().values.tolist())

# Iterate over the DataFrame to add markers and set the marker color based on the count
for index, row in df.iterrows():
    city = row['città']
    count = city_counts_dict[city]
    color = 'red' if count > 10 else 'green' if count > 50 else 'blue'
    folium.Marker([row['lat'], row['lon']], icon=folium.Icon(color=color), popup=f"{city}: {count}").add_to(m)

# Iterate over the DataFrame to add markers and set the icon based on the region
for lat, lon, regione in zip(df_cord['lat'], df_cord['lon'], df_cord['regione']):
    # use the icon function of Folium to specify the icon to use for each marker
    folium.Marker(location=[lat, lon], icon=folium.Icon(icon=icon_dict[regione])).add_to(m)

# Fit the map bounds and save it to an HTML file
sw = df_cord[['lat', 'lon']].min().values.tolist()
ne = df_cord[['lat', 'lon']].max().values.tolist()
m.fit_bounds([sw, ne])
m.save('map3.html')

# Display the map in a Jupyter notebook using an IFrame
IFrame(src='map3.html', width='100%', height='500px')


"""

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
