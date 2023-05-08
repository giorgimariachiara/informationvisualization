"""
# Import libraries
import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt





# Load the Italian regions shapefile
italy_regions = gpd.read_file('https://raw.githubusercontent.com/openpolis/geojson-italy/master/geojson/limits_IT_regions.geojson')

# Load the data of Italian deputies by city and gender
deputies = pd.read_csv('deputies.csv')

# Group the deputies data by region and gender
deputies_grouped = deputies.groupby(['regione', 'gender']).size().reset_index(name='count')

# Merge the deputies data with the regions shapefile
italy_regions_deputies = italy_regions.merge(deputies_grouped, left_on='reg_name', right_on='regione')

# Create two separate GeoDataFrames for male and female deputies
deputies_male = italy_regions_deputies[italy_regions_deputies['gender'] == 'male']
deputies_female = italy_regions_deputies[italy_regions_deputies['gender'] == 'female']

# Create a figure with two subplots
fig, ax = plt.subplots(1, 2, figsize=(20, 10))

# Plot the male deputies on the left subplot
deputies_male.plot(column='count', cmap='Blues', linewidth=0.8, edgecolor='0.8', ax=ax[0], legend=True)
ax[0].set_title('Provenance of Male Deputies')
ax[0].set_axis_off()
ax[0].set_aspect('equal')

# Plot the female deputies on the right subplot
deputies_female.plot(column='count', cmap='Reds', linewidth=0.8, edgecolor='0.8', ax=ax[1], legend=True)
ax[1].set_title('Provenance of Female Deputies')
ax[1].set_axis_off()
ax[1].set_aspect('equal')

# Remove the axis
ax[0].set_axis_off()
ax[1].set_axis_off()


# Add a title
fig.suptitle('Provenance  of Italian Deputies by Gender', fontsize=20)

# Show the plot
plt.show()

"""


import pandas as pd
import geopandas as gpd
from geopy.geocoders import GoogleV3
import matplotlib.pyplot as plt

# Load the data
deputies = pd.read_csv('deputies.csv')

# Get the latitude and longitude of each city
API_KEY = 'AIzaSyBnYsnoSGLS6-bd-6rzBKvMhDigOVGcFLs'
geolocator = GoogleV3(api_key=API_KEY)
deputies['Location'] = deputies['città'] + ', Italy'
deputies['Latitude'] = deputies['Location'].apply(lambda x: geolocator.geocode(x).latitude)
deputies['Longitude'] = deputies['Location'].apply(lambda x: geolocator.geocode(x).longitude)

# Create a pivot table to count the number of deputies by city and gender
pivot = pd.pivot_table(deputies, index=['città'], columns=['Gender'], values=['Name'], aggfunc='count', fill_value=0)

# Rename the columns
pivot.columns = ['Female', 'Male']

# Load the world map and plot the data
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
italy = world[world.name == 'Italy']
ax = italy.plot(color='white', edgecolor='black', figsize=(10, 10))

for idx, row in pivot.iterrows():
    city = idx
    lat, lon = row['Latitude'], row['Longitude']
    size = (row['Male'] + row['Female']) * 10
    color = 'blue' if row['Male'] > row['Female'] else 'red'
    ax.scatter(lon, lat, s=size, color=color, alpha=0.5)
    ax.annotate(city, (lon, lat), fontsize=10, fontweight='bold')

plt.show()