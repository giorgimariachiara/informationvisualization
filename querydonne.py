from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get
from bs4 import BeautifulSoup
import requests 

endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)

#QUERY NUMERO TOTALE DONNE 905  

totale_donne = """
SELECT DISTINCT ?persona ?cognome ?nome
?dataNascita ?luogoNascita "female" as ?gender 
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
df_totale_donne = sparql_dataframe.get(endpoint, totale_donne)
df_totale_donne = df_totale_donne[['nome', 'cognome', 'gender']]

#QUERY NUMERO TOTALE UOMINI 5204 
totale_uomini = """
SELECT DISTINCT ?persona ?cognome ?nome
?dataNascita ?luogoNascita "male" as ?gender
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
?d a ocd:deputato; 
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.
OPTIONAL{?d dc:description ?info}

##anagrafica
?d foaf:surname ?cognome; foaf:gender "male" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.
}}"""
df_totale_uomini = sparql_dataframe.get(endpoint, totale_uomini)
df_totale_uomini = df_totale_uomini[['nome', 'cognome', 'gender']]

df_totale = pd.concat([df_totale_uomini, df_totale_donne])
#df_totale.to_csv("totaledeputati.csv",  index=False, index_label=False)  #6109

#QUERY PER CITTà NASCITA 
donne_nascita = """
SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?regione
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
?luogoNascitaUri ocd:parentADM3 ?regione. 
}}"""

df_donne_nascita = get(endpoint, donne_nascita)
df_donne_nascita.rename(columns={"luogoNascita": "città"}, inplace=True)
df_donne_nascita = df_donne_nascita[["città", "regione"]]
#df_donne_nascita.to_csv("donnemappa.csv",  index=False, index_label=False)

#QUERY PARTITO DONNE 
partito_donne = """
SELECT DISTINCT ?persona ?cognome ?nome
?dataNascita ?luogoNascita ?gruppoPar
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.


?d a ocd:deputato.
?d ocd:rif_leg ?legislatura.
?d ocd:aderisce ?gruppo.
?d ocd:rif_mandatoCamera ?mandato.
?gruppo rdfs:label ?gruppoPar.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.
}}"""
df_donne_partito = get(endpoint, partito_donne)

print(len(df_donne_partito))

query_donne0 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome "female" as ?gender where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>.
 }
     
"""
dfemale0 = get(endpoint, query_donne0) 

#QUERY DONNE LEGISLATURA 1  
query_donne1 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>.
 }
  
"""
dfemale1 = get(endpoint, query_donne1)


#QUERY DONNE LEGISLATURA 2  
query_donne2 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>.
 }
     
"""

dfemale2 = get(endpoint, query_donne2)


#QUERY DONNE LEGISLATURA 3  
query_donne3 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>.
 }
     
"""

dfemale3 = get(endpoint, query_donne3)

#QUERY DONNE LEGISLATURA 4  
query_donne4 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>.
 }
     
"""

dfemale4 = get(endpoint, query_donne4)

#QUERY DONNE LEGISLATURA 5  
query_donne5 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>.
 }
     
"""

dfemale5 = get(endpoint, query_donne5)

#QUERY UOMINI LEGISLATURA 6  
query_donne6 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>.
 }
     
"""
dfemale6 = get(endpoint, query_donne6)

#QUERY DONNE LEGISLATURA 7 
query_donne7 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>.
 }
     
"""

dfemale7 = get(endpoint, query_donne7)

#QUERY DONNE LEGISLATURA 8  
query_donne8 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>.
 }
     
"""

dfemale8 = sparql_dataframe.get(endpoint, query_donne8)

#QUERY DONNE LEGISLATURA 9  
query_donne9 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>.
 }
     
"""

dfemale9 = get(endpoint, query_donne9)

#QUERY DONNE LEGISLATURA 10 
query_donne10 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>.
 }
     
"""

dfemale10 = get(endpoint, query_donne10)

#QUERY DONNE LEGISLATURA 11  
query_donne11 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>.
 }
     
"""

dfemale11 = get(endpoint, query_donne11)

#QUERY DONNE LEGISLATURA 12  
query_donne12 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>.
 }
     
"""

dfemale12 = get(endpoint, query_donne12)

#QUERY DONNE LEGISLATURA 13 
query_donne13 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>.
 }
     
"""

dfemale13 = get(endpoint, query_donne13)

#QUERY DONNE LEGISLATURA 14 
query_donne14 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>.
 }
     
"""

dfemale14 = get(endpoint, query_donne14)

#QUERY DONNE LEGISLATURA 15  
query_donne15 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>.
 }
     
"""

dfemale15 = get(endpoint, query_donne15)

#QUERY DONNE LEGISLATURA 16  
query_donne16 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>.
 }
"""

dfemale16 = get(endpoint, query_donne16)


#QUERY DONNE LEGISLATURA 17 
query_donne17 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>.
 }
     
"""

dfemale17 = get(endpoint, query_donne17)


#QUERY DONNE LEGISLATURA 18 
query_donne18 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>.
 }

     
"""

dfemale18 = get(endpoint, query_donne18)


#QUERY DONNE LEGISLATURA 19 
query_donne19 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "female".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>.
 }
 """
dfemale19 = get(endpoint, query_donne19)


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
df_nana = df_nana[["nome", "cognome", "luogoNascita"]]
nomi = df_nana['nome'] + ' ' + df_nana['cognome']
nomidonne = nomi.to_list()
dataprova['info'] = dataprova['info'].fillna('')
mask = dataprova['info'].str.contains('Laurea|laurea|Master|LAUREA')

# Estrarre le righe che soddisfano la maschera
donnelaureate = dataprova[mask]
nonlaureate = ~dataprova['info'].str.contains('Laurea|laurea|Master|LAUREA', na=False) & dataprova['info'].ne('')
nonlaureate = dataprova[nonlaureate]
nonlaureate = nonlaureate.assign(info="no")
nonlaureate = nonlaureate.assign(gender='female')

nonlaureate = nonlaureate[["info", "gender"]]
donnenonlaureate = nonlaureate.rename(columns={'info': 'graduated'})
# Stampa del dataframe risultante 287 non laureate, 569 si , 49 non si sa 

#df_nanadonne = dataprova[dataprova['info'].isna()] #qui le donne senza info diventano solo 49 
#df_risultati = dataprova.loc[(dataprova['nome'] == "ELISABETTA") & (dataprova['cognome'] == "GARDINI")]
"""
donnelaureate = donnelaureate.assign(info="yes")
donnelaureate = donnelaureate.assign(gender='female')

donnelaureate = donnelaureate[["info", "gender"]]
donnelaureate = donnelaureate.rename(columns={'info': 'graduated'})
donnelaureacsv = pd.concat([donnelaureate, donnenonlaureate],  axis=0)
donnelaureacsv.to_csv("womengraduation.csv",  index=False, index_label=False)
#print(len(donnenonlaureate))
dfmen = pd.read_csv('./mengraduation.csv')

# Leggi il secondo file CSV
dfwomen = pd.read_csv('./womengraduation.csv')

# Unisci i due dataframe in base alle colonne uguali
merged_df = pd.concat([dfmen, dfwomen], axis=0)

# Salva il risultato in un nuovo file CSV
merged_df.to_csv('graduation.csv', index=False, index_label=False)
"""
def get_uri_from_names(lista):
    # Inizializza l'oggetto SPARQLWrapper
    sparql = SPARQLWrapper("http://tuo_endpoint_sparql")  
    risultati = []
    for nome_cognome in lista:
        # Costruisci la query SPARQL
        query = '''
            SELECT DISTINCT ?persona ?nome ?cognome ?uri
            WHERE {{
                ?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
                ?persona foaf:firstName ?nome.
                ?persona foaf:surname ?cognome. 
                ?persona owl:sameAs ?uri.
                FILTER (CONCAT(?nome, " ", ?cognome) = "{0}")
            }}
        '''.format(nome_cognome)

        queryy = get(endpoint, query)
        risultati.append(queryy)

    # Unisci tutti i dataframe in un unico dataframe finale
    df_finale = pd.concat(risultati)
    return df_finale  
da = get_uri_from_names(nomidonne)
da = da.drop_duplicates(["persona", "nome", "cognome"])
da = da[["nome", "cognome"]]
merged = pd.merge(da, df_nana, how='outer', indicator=True)
filtered = merged[merged['_merge'] != 'both']

# Risultato finale
result = filtered.drop('_merge', axis=1)
result = result[["nome", "cognome"]]
#print(result.values.tolist())
#print(len(result))

new_df = dfemale0.loc[:, ['nome', 'cognome']]


new_df1 = dfemale1.loc[:, ['nome', 'cognome']]


new_df2 = dfemale2.loc[:, ['nome', 'cognome']]


new_df3 = dfemale3.loc[:, ['nome', 'cognome']]


new_df4 = dfemale4.loc[:, ['nome', 'cognome']]


new_df5 = dfemale5.loc[:, ['nome', 'cognome']]


new_df6 = dfemale6.loc[:, ['nome', 'cognome']]


new_df7 = dfemale7.loc[:, ['nome', 'cognome']]


new_df8 = dfemale8.loc[:, ['nome', 'cognome']]


new_df9 = dfemale9.loc[:, ['nome', 'cognome']]


new_df10 = dfemale10.loc[:, ['nome', 'cognome']]


new_df11 = dfemale11.loc[:, ['nome', 'cognome']]


new_df12 = dfemale12.loc[:, ['nome', 'cognome']]


new_df13= dfemale13.loc[:, ['nome', 'cognome']]


new_df14= dfemale14.loc[:, ['nome', 'cognome']]


new_df15= dfemale15.loc[:, ['nome', 'cognome']]


new_df16= dfemale16.loc[:, ['nome', 'cognome']]


new_df17= dfemale17.loc[:, ['nome', 'cognome']]


new_df18= dfemale18.loc[:, ['nome', 'cognome']]


new_df19= dfemale19.loc[:, ['nome', 'cognome']]

merged_df = pd.concat([new_df, new_df1, new_df2, new_df3, new_df4, new_df5, new_df6, new_df7, new_df8, new_df9, new_df10, new_df11, new_df12, new_df13, new_df14, new_df15, new_df16, new_df17, new_df18, new_df19], axis=0)

#merged_dfinal = merged_df.drop_duplicates()
#print(len(merged_df))