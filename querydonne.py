from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get

endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)

querydonne0 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>.
 }
     
"""
dfemale0 = get(endpoint, querydonne0)

#QUERY UOMINI LEGISLATURA 1  
querydonne1 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>.
 }
  
"""

dfemale1 = get(endpoint, querydonne1)

#QUERY UOMINI LEGISLATURA 2  
querydonne2 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>.
 }
     
"""

dfemale2 = get(endpoint, querydonne2)


#QUERY UOMINI LEGISLATURA 3  
querydonne3 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>.
 }
     
"""

dfemale3 = get(endpoint, querydonne3)

#QUERY UOMINI LEGISLATURA 4  
querydonne4 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>.
 }
     
"""

dfemale4 = get(endpoint, querydonne4)

#QUERY UOMINI LEGISLATURA 5  
querydonne5 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>.
 }
     
"""

dfemale5 = get(endpoint, querydonne5)

#QUERY UOMINI LEGISLATURA 6  
querydonne6 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>.
 }
     
"""
dfemale6 = get(endpoint, querydonne6)

#QUERY UOMINI LEGISLATURA 7 
querydonne7 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>.
 }
     
"""

dfemale7 = get(endpoint, querydonne7)

#QUERY UOMINI LEGISLATURA 8  
querydonne8 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>.
 }
     
"""

dfemale8 = sparql_dataframe.get(endpoint, querydonne8)

#QUERY UOMINI LEGISLATURA 9  
querydonne9 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>.
 }
     
"""

dfemale9 = get(endpoint, querydonne9)

#QUERY UOMINI LEGISLATURA 10 
querydonne10 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>.
 }
     
"""

dfemale10 = get(endpoint, querydonne10)

#QUERY UOMINI LEGISLATURA 11  
querydonne11 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>.
 }
     
"""

dfemale11 = get(endpoint, querydonne11)

#QUERY UOMINI LEGISLATURA 12  
querydonne12 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>.
 }
     
"""

dfemale12 = get(endpoint, querydonne12)

#QUERY UOMINI LEGISLATURA 13 
querydonne13 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>.
 }
     
"""

dfemale13 = get(endpoint, querydonne13)

#QUERY UOMINI LEGISLATURA 14 
querydonne14 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>.
 }
     
"""

dfemale14 = get(endpoint, querydonne14)

#QUERY UOMINI LEGISLATURA 15  
querydonne15 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>.
 }
     
"""

dfemale15 = get(endpoint, querydonne15)

#QUERY UOMINI LEGISLATURA 16  
querydonne16 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>.
 }
"""

dfemale16 = get(endpoint, querydonne16)


#QUERY UOMINI LEGISLATURA 17 
querydonne17 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>.
 }
     
"""

dfemale17 = get(endpoint, querydonne17)


#QUERY UOMINI LEGISLATURA 18 
querydonne18 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>.
 }

     
"""

dfemale18 = get(endpoint, querydonne18)


#QUERY UOMINI LEGISLATURA 19 
querydonne19 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>.
 }
 """
dfemale19 = get(endpoint, querydonne19)

"""

dataframes = [dfemale0, dfemale1, dfemale2, dfemale3, dfemale4, dfemale5, dfemale6, dfemale7, dfemale8, dfemale9, dfemale10, dfemale11, dfemale12, dfemale13, dfemale14, dfemale15, dfemale16, dfemale17, dfemale18, dfemale19 ]

# creazione della lista per salvare i dataframe finali
new_dfs = []

# ciclo for per elaborare ogni dataframe nella lista
for df in dataframes:
    new_df = df.loc[:, ['nome', 'cognome']].drop_duplicates()
    new_dfs.append(new_df)

# unione di tutti i dataframe finali in un unico dataframe
merged_df = pd.concat(new_dfs, axis=0)
merged_dfinal = merged_df.drop_duplicates()

# stampa del dataframe risultante
#print(merged_dfinal)
print(len(merged_dfinal))

"""
"""
new_df = dfemale0.loc[:, ['nome', 'cognome']]
new_df = new_df.drop_duplicates()

new_df1 = dfemale1.loc[:, ['nome', 'cognome']]
new_df1 = new_df1.drop_duplicates()

new_df2 = dfemale2.loc[:, ['nome', 'cognome']]
new_df2 = new_df2.drop_duplicates()

new_df3 = dfemale3.loc[:, ['nome', 'cognome']]
new_df3 = new_df3.drop_duplicates()

new_df4 = dfemale4.loc[:, ['nome', 'cognome']]
new_df4 = new_df4.drop_duplicates()

new_df5 = dfemale5.loc[:, ['nome', 'cognome']]
new_df5= new_df5.drop_duplicates()

new_df6 = dfemale6.loc[:, ['nome', 'cognome']]
new_df6 = new_df6.drop_duplicates()

new_df7 = dfemale7.loc[:, ['nome', 'cognome']]
new_df7= new_df7.drop_duplicates()

new_df8 = dfemale8.loc[:, ['nome', 'cognome']]
new_df8= new_df8.drop_duplicates()

new_df9 = dfemale9.loc[:, ['nome', 'cognome']]
new_df9= new_df9.drop_duplicates()

new_df10 = dfemale10.loc[:, ['nome', 'cognome']]
new_df10= new_df10.drop_duplicates()

new_df11 = dfemale11.loc[:, ['nome', 'cognome']]
new_df11= new_df11.drop_duplicates()

new_df12 = dfemale12.loc[:, ['nome', 'cognome']]
new_df12= new_df12.drop_duplicates()

new_df13= dfemale13.loc[:, ['nome', 'cognome']]
new_df13= new_df13.drop_duplicates()

new_df14= dfemale14.loc[:, ['nome', 'cognome']]
new_df14= new_df14.drop_duplicates()

new_df15= dfemale15.loc[:, ['nome', 'cognome']]
new_df15= new_df15.drop_duplicates()

new_df16= dfemale16.loc[:, ['nome', 'cognome']]
new_df16= new_df16.drop_duplicates()

new_df17= dfemale17.loc[:, ['nome', 'cognome']]
new_df17= new_df17.drop_duplicates()

new_df18= dfemale18.loc[:, ['nome', 'cognome']]
new_df18= new_df18.drop_duplicates()

new_df19= dfemale19.loc[:, ['nome', 'cognome']]
new_df19= new_df19.drop_duplicates()
merged_df = pd.concat([new_df, new_df1, new_df2, new_df3, new_df4, new_df5, new_df6, new_df7, new_df8, new_df9, new_df10, new_df11, new_df12, new_df13, new_df14, new_df15, new_df16, new_df17, new_df18, new_df19], axis=0)

merged_dfinal = merged_df.drop_duplicates()
"""

querylaureadonnetutte = """SELECT distinct ?nome ?cognome ?descrizione ?luogoNascita where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type foaf:Person. 
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg ?legislatura.
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
             rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri dc:title ?luogoNascita.
  OPTIONAL {?persona dc:description ?descrizione.}
  
 }
"""
datalaureadonne = get(endpoint, querylaureadonnetutte)

df_nan = datalaureadonne[datalaureadonne['descrizione'].isna()] #49 donne non hanno la descrizione 294 senza laurea 572 con laurea 


queryprovaa ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
 ?luogoNascita 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

?d a ocd:deputato; 
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.
OPTIONAL{?d dc:description ?info}

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.
}}"""
dataprova = get(endpoint, queryprovaa)
dataprova = dataprova.drop_duplicates(["persona","nome", "cognome", "luogoNascita"])
dataprova = dataprova[["nome","cognome","info", "luogoNascita"]]
df_nana = dataprova[dataprova['info'].isnull()] #qui le donne senza info diventano solo 49 
dataprova['info'] = dataprova['info'].fillna('')
mask = dataprova['info'].str.contains('Laurea|laurea|Master|LAUREA')

# Estrarre le righe che soddisfano la maschera
donnelaureate = dataprova[mask]
nonlaureate = ~dataprova['info'].str.contains('Laurea|laurea|Master|LAUREA', na=False) & dataprova['info'].ne('')
nonlaureate = dataprova[nonlaureate]
nonlaureate = nonlaureate.assign(info="yes")
nonlaureate = nonlaureate.assign(gender='female')

nonlaureate = nonlaureate[["info", "gender"]]
donnenonlaureate = nonlaureate.rename(columns={'info': 'graduated'})
# Stampa del dataframe risultante 287 non laureate, 569 si , 49 non si sa 

#df_nanadonne = dataprova[dataprova['info'].isna()] #qui le donne senza info diventano solo 49 
#df_risultati = dataprova.loc[(dataprova['nome'] == "ELISABETTA") & (dataprova['cognome'] == "GARDINI")]

donnelaureate = donnelaureate.assign(info="yes")
donnelaureate = donnelaureate.assign(gender='female')

donnelaureate = donnelaureate[["info", "gender"]]
donnelaureate = donnelaureate.rename(columns={'info': 'graduated'})
donnelaureacsv = pd.concat([donnelaureate, donnenonlaureate],  axis=0)
donnelaureacsv.to_csv("womengraduation.csv",  index=False, index_label=False)
#print(len(donnenonlaureate))

#donnelaureate.to_csv("graduated.csv", index=False, index_label=False) 



