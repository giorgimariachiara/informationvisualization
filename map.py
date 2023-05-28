from geopy.geocoders import Nominatim
import folium
import pandas as pd
from IPython.display import IFrame
from geopy.exc import GeocoderTimedOut
from IPython.display import display
from IPython.display import HTML
from functools import lru_cache
from folium.plugins import MarkerCluster

# Create an LRUCache class
class LRUCache:
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.cache = {}

    def __getitem__(self, key):
        if key in self.cache:
            value = self.cache.pop(key)
            self.cache[key] = value  # Move the item to the end to mark it as most recently used
            return value

    def __setitem__(self, key, value):
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.maxsize:
            self.cache.pop(next(iter(self.cache)))  # Remove the least recently used item
        self.cache[key] = value

# Create a function to geocode with a timeout feature and cache the results
@lru_cache(maxsize=1000)
def geocode_with_cache(loc, query, timeout=5, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            return loc.geocode(query, timeout=timeout)
        except GeocoderTimedOut:
            retries += 1
    return None

# Read the CSV file
df = pd.read_csv('uominimappa.csv')

# Create a dictionary to map region to icon color
icon_dict = {'Nord': 'blue', 'Centro': 'green', 'Sud': 'red'}
region_dict = {
    'PIEMONTE': 'Nord',
    'VALLE D\'AOSTA': 'Nord',
    'LOMBARDIA': 'Nord',
    'TRENTINO ALTO-ADIGE': 'Nord',
    'VENETO': 'Nord',
    'FRIULI-VENEZIA-GIULIA': 'Nord',
    'LIGURIA': 'Nord',
    'EMILIA-ROMAGNA': 'Nord',
    'TOSCANA': 'Centro',
    'UMBRIA': 'Centro',
    'MARCHE': 'Centro',
    'LAZIO': 'Centro',
    'ABRUZZO': 'Sud',
    'MOLISE': 'Sud',
    'CAMPANIA': 'Sud',
    'PUGLIA': 'Sud',
    'BASILICATA': 'Sud',
    'CALABRIA': 'Sud',
    'SICILIA': 'Sud',
    'SARDEGNA': 'Sud'
}

# Calculate the city counts
city_counts_dict = df['città'].value_counts().to_dict()

# Calling the Nominatim tool
geolocator = Nominatim(user_agent='mariachiara.giorgi1@gmail.com')

# Create empty lists to store latitude, longitude, and region
lat = []
lon = []
regione = []

# Create a cache with a max size of 1000
geocode_cache = LRUCache(maxsize=1000)

# Create a dictionary to store the geocoded locations
geocoded_locations = {}

# Iterate over the DataFrame to geocode the cities and get the coordinates
for index, row in df.iterrows():
    city = row['città']
    if city in geocoded_locations:
        getLoc = geocoded_locations[city]
    else:
        getLoc = geocode_with_cache(geolocator, city)
        geocoded_locations[city] = getLoc
    
    if getLoc is not None:
        lat.append(getLoc.latitude)
        lon.append(getLoc.longitude)
        regione.append(row['regione'])
    else:
        print(f"No coordinates found for city {city} in row {index}")
        lat.append(None)
        lon.append(None)
        regione.append(None)

# Create a DataFrame to store the coordinates and region
df_cord = pd.concat([pd.Series(lat, name='lat'), pd.Series(lon, name='lon'), pd.Series(regione, name='regione')], axis=1)
df['lat'] = lat
df['lon'] = lon

# Create a folium map
m = folium.Map(df_cord[['lat', 'lon']].mean().values.tolist(), zoom_start=5)

# Create a MarkerCluster object
marker_cluster = MarkerCluster().add_to(m)

# Initialize a dictionary to store city markers
city_markers = {}

for index, row in df.iterrows():
    city = row['città']
    if pd.isna(city):
        continue  # Skip the iteration if city is NaN

    lat = row['lat']
    lon = row['lon']
    if pd.isna(lat) or pd.isna(lon):
        continue  # Skip the iteration if lat or lon is NaN

    count = city_counts_dict.get(city, 0)  # Use dictionary.get() to handle missing keys
    color = 'red' if count > 30 else 'green' if count > 15 else 'blue' if count > 5 else 'black'

    regione = row['regione']
    if pd.notna(regione):
        if region_dict[regione] == 'Nord':
            marker_icon = folium.Icon(icon='cloud', color=color)
        elif region_dict[regione] == 'Centro':
            marker_icon = folium.Icon(icon='leaf', color=color)
        else:
            marker_icon = folium.Icon(icon='star', color=color)

        # Check if the city marker already exists
        if city in city_markers:
            # Update the count on the existing marker
            existing_marker = city_markers[city]
            existing_count = existing_marker.options.get('popup').split(":")[1].strip()
            new_count = int(existing_count) + count
            existing_marker.options['popup'] = f"{city}: {new_count}"
        else:
            # Create a new marker for the city
            marker = folium.Marker(
                location=[lat, lon],
                icon=marker_icon,
                popup=f"{city}: {count}",
                tooltip=city
            )
            # Add the marker to the city_markers dictionary
            city_markers[city] = marker
            marker.add_to(marker_cluster)
    else:
        # Handle missing regione value
        # You can choose to skip or add a default marker/icon here
        pass

# Add the marker cluster to the map
marker_cluster.add_to(m)

# Fit the map bounds and save it to an HTML file
sw = df_cord[['lat', 'lon']].min().values.tolist()
ne = df_cord[['lat', 'lon']].max().values.tolist()
m.fit_bounds([sw, ne])
m.save('map.html')

# Display the map in a Jupyter notebook using an IFrame
display(IFrame(src='map.html', width=800, height=500))




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
