import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from shapely.geometry import Point
import matplotlib.pyplot as plt


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





