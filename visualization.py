import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from shapely.geometry import Point
import matplotlib.pyplot as plt

from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe

 


endpoint = "https://dati.camera.it/sparql"

q4 = """select ?nome ?cognome ?città?regione where {
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
dataframecities = df["città"]

# count the frequency of each city and create a new dataframe
city_counts = df['città'].value_counts().reset_index()
city_counts.columns = ['città', 'count']

# write the result to a CSV file
city_counts.to_csv('cities.csv', index=False)


# Read in the CSV file containing the deputies data
deputies = pd.read_csv('deputies.csv')

# Create a GeoDataFrame from the deputies data
geocoded_cities = []
geolocator = Nominatim(user_agent='GetLoc')
for city in deputies['città']:
    location = geolocator.geocode(city, exactly_one=True, timeout=10)
    if location is not None:
        geocoded_cities.append({'città': city, 'Latitude': location.latitude, 'Longitude': location.longitude})

cities_df = pd.DataFrame(geocoded_cities)
geometry = [Point(xy) for xy in zip(cities_df['Longitude'], cities_df['Latitude'])]
crs = {'init': 'epsg:4326'}
cities_gdf = gpd.GeoDataFrame(cities_df, crs=crs, geometry=geometry)
cities_gdf = cities_gdf[cities_gdf['Latitude'].notna() & cities_gdf['Longitude'].notna()]

# Create a choropleth map with markers for each city, colored by gender
ax = cities_gdf.plot(column='genere', categorical=True, legend=True, markersize=50, cmap='Set1', figsize=(10, 10))

ax.set_title('Cities of Origin for Deputies in the Italian Chamber of Deputies, by Gender')
ax.set_axis_off()

plt.show()




