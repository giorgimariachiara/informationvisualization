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
df_totale_uomini = df_totale_uomini[['nome', 'cognome', 'gender', "dataNascita"]]
df_totale_uomini = df_totale_uomini.drop_duplicates()
print(len(df_totale_uomini))
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
