# Import libraries
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Load the Italian regions shapefile
italy_regions = gpd.read_file('https://storage.googleapis.com/italy_regions_geojson/italy_regions.geojson')

# Load the data of Italian deputies by city and gender
deputies = pd.read_csv('deputies.csv')

# Group the deputies data by region and gender
deputies_grouped = deputies.groupby(['region', 'gender']).size().reset_index(name='count')

# Merge the deputies data with the regions shapefile
italy_regions_deputies = italy_regions.merge(deputies_grouped, left_on='reg_name', right_on='region')

# Create two separate GeoDataFrames for male and female deputies
deputies_male = italy_regions_deputies[italy_regions_deputies['gender'] == 'M']
deputies_female = italy_regions_deputies[italy_regions_deputies['gender'] == 'F']

# Create a figure with two subplots
fig, ax = plt.subplots(1, 2, figsize=(20, 10))

# Plot the male deputies on the left subplot
deputies_male.plot(column='count', cmap='Blues', linewidth=0.8, edgecolor='0.8', ax=ax[0], legend=True)
ax[0].set_title('Provenance of Male Deputies')

# Plot the female deputies on the right subplot
deputies_female.plot(column='count', cmap='Reds', linewidth=0.8, edgecolor='0.8', ax=ax[1], legend=True)
ax[1].set_title('Provenance of Female Deputies')

# Remove the axis
ax[0].set_axis_off()
ax[1].set_axis_off()

# Add a title
fig.suptitle('Provenance of Italian Deputies by Gender', fontsize=20)

# Show the plot
plt.show()
