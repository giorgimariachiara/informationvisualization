import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe
from shapely.geometry import Point

fp =  'italy_provinces.shp'
#reading the file stored in variable fp
map_df = gpd.read_file(fp)

points_df = map_df.copy()
# change geometry 
points_df['geometry'] = points_df['geometry'].centroid
newpoints_df = points_df.drop(columns=['PROVINCIA'])
newpoints_df = newpoints_df.rename(index=str, columns={'DEN_CMPRO': 'PROVINCIA'})


#opening the csv(.shp) file which contains the data to be plotted on the map
df = pd.read_csv('femalecities.csv')


#selecting the columns required
df = df[['città', 'count']]
#renaming the column name
data_for_map = df.rename(index=str, columns={'città': 'PROVINCIA', 'count': 'tot'})


# joining the geodataframe with the cleaned up csv dataframe
#merged = data_for_map.merge(newpoints_df, on='PROVINCIA')

merged = newpoints_df.set_index('PROVINCIA').join(data_for_map.set_index('PROVINCIA'))

print(merged.columns)


"""

# set a variable that will call whatever column we want to visualise on the map
variable = 'PROVINCIA'
# set the range for the choropleth
vmin, vmax = 0, 500

# create figure and axes for Matplotlib
fig, ax = plt.subplots(1, figsize=(10, 6))



points_df.plot(ax=ax, color='white', edgecolor='black')
ax.scatter(merged.geometry.x, merged.geometry.y, c=merged['tot'], cmap='BuGn', vmin=vmin, vmax=vmax, alpha=0.7, edgecolor='black')
#merged.plot(column=variable, cmap='BuGn', linewidth=0.8, ax=ax, edgecolor='0.8')

# remove the axis
ax.axis('off')
# add a title
ax.set_title('Provenience of Deputies', fontdict={'fontsize': '25', 'fontweight' : '3'})
# create an annotation for the data source
ax.annotate('Source: Istat',xy=(0.1, .08), xycoords='figure fraction', horizontalalignment='left', verticalalignment='top', fontsize=12, color='#555555')


# Create colorbar as a legend
sm = plt.cm.ScalarMappable(cmap='BuGn', norm=plt.Normalize(vmin=vmin, vmax=vmax))
# empty array for the data range
sm._A = []
# add the colorbar to the figure
#cbar = fig.colorbar(sm)
cax = fig.add_axes([0.27, 0.1, 0.5, 0.03]) # define the position of the colorbar
cbar = fig.colorbar(sm, orientation='horizontal', cax=cax)


#plt.show()

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
