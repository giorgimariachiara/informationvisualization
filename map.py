from geopy.geocoders import Nominatim
import folium
import pandas as pd
from IPython.display import IFrame
from geopy.exc import GeocoderTimedOut
from IPython.display import display
from IPython.display import HTML


# Create a function to geocode with a timeout feature
def geocode_with_timeout(loc, query, timeout=5, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            return loc.geocode(query, timeout=timeout)
        except GeocoderTimedOut:
            retries += 1
    return None

# Read the CSV file
df = pd.read_csv('deputies.csv')

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
loc = Nominatim(user_agent='mariachiara.giorgi1@gmail.com')
# Create empty lists to store latitude, longitude, and region
lat = []
lon = []
regione = []

# Iterate over the DataFrame to get location coordinates and region
for index, row in df.iterrows():
    getLoc = geocode_with_timeout(loc, row['città'])
    if getLoc is not None:
        lat.append(getLoc.latitude)
        lon.append(getLoc.longitude)
        regione.append(row['regione'])
    else:
        print(f"No coordinates found for city {row['città']} in row {index}")
        lat.append(None)
        lon.append(None)
        regione.append(None)


# Create a DataFrame to store the coordinates and region
df_cord = pd.concat([pd.Series(lat, name='lat'), pd.Series(lon, name='lon'), pd.Series(regione, name='regione')], axis=1)
df['lat'] = lat
df['lon'] = lon

headers = {'user-Agent': 'mariachiara.giorgi1@gmail.com'}
# Create a folium map
m = folium.Map(df_cord[['lat', 'lon']].mean().values.tolist())


#In this order, if the count is greater than 50, the marker color will be red. If the count is between 11 and 50, the color will be green. If the count is between 6 and 10, the color will be blue. If the count is less than or equal to 5, the color will be black.    folium.Marker([row['lat'], row['lon']], icon=folium.Icon(color=color), popup=f"{city}: {count}").add_to(m)
# Iterate over the DataFrame to add markers and set the icon based on the region
for index, row in df.iterrows():
    city = row['città']
    count = city_counts_dict[city]
    color = 'red' if count > 50 else 'green' if count > 10 else 'blue' if count > 5 else 'black'
    folium.Marker([row['lat'], row['lon']], icon=folium.Icon(color=color), popup=f"{city}: {count}").add_to(m)   
   
    regione = row['regione']
    for regione in df_cord['regione']:
        if pd.notna(regione):
            if region_dict[regione] == 'Nord':
                marker_icon = folium.Icon(icon='cloud')
            elif region_dict[regione] == 'Centro':
                marker_icon = folium.Icon(icon='leaf')
            else:
                marker_icon = folium.Icon(icon='star')
        
            lat = row['lat']
            lon = row['lon']
            folium.Marker(location=[lat, lon], icon=marker_icon, popup=f"{city}: {count}", tooltip=city, color=color).add_to(m)
        else:
            # Handle missing regione value
            # You can choose to skip or add a default marker/icon here
            pass
# Fit the map bounds and save it to an HTML file
sw = df_cord[['lat', 'lon']].min().values.tolist()
ne = df_cord[['lat', 'lon']].max().values.tolist()
m.fit_bounds([sw, ne])
m.save('map3.html')

# Display the map in a Jupyter notebook using an IFrame
display(IFrame(src='map3.html', width='100%', height='500px'))

HTML(m._repr_html_())

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
