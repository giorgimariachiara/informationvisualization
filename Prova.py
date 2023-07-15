import pandas as pd
from json import load
from pandas import DataFrame
from pandas import concat, read_sql
import csv
pd.set_option('display.max_rows', None)

from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe
from sparql_dataframe import get

endpoint = "https://dati.camera.it/sparql"

#QUERY CARICA DONNE 
querycaricadonne = """SELECT DISTINCT ?nome ?cognome ?ufficio ?organo where {
  
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome.
  ?persona foaf:surname ?cognome. 
  ?persona ocd:rif_ufficioParlamentare ?ufficioUri.
  ?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.
  ?organoUri dc:title ?organo.
  
 } """

dfcaricadonne = sparql_dataframe.get(endpoint, querycaricadonne)


queryincaricodonneee = """SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?ufficio 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female";foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri. 
?luogoNascitaUri dc:title ?luogoNascita. 
}
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.


} """

queryincaricodeputatedonne = """ SELECT DISTINCT ?d ?legislatura ?cognome ?nome 
  ?ufficio
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.


}    """

df_incarico_donne = get(endpoint, queryincaricodeputatedonne)
df_incarico_donne = df_incarico_donne[["nome", "cognome", "legislatura", "ufficio"]]
df_incarico_donne = df_incarico_donne.rename(columns={'ufficio': 'incarico'})
#df_incarico_donne.to_csv("incaricodonne.csv",  index=False, index_label=False)


queryincaricodeputatiuomini = """
SELECT DISTINCT ?d ?legislatura ?cognome ?nome 
  ?ufficio
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "male"; foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.
}   """

df_incarico_uomini = get(endpoint, queryincaricodeputatiuomini)
df_incarico_uomini = df_incarico_uomini[["nome", "cognome", "legislatura", "ufficio"]]
df_incarico_uomini = df_incarico_uomini.rename(columns={'ufficio': 'incarico'})

queryincaricodeputatiuominigp = """
SELECT DISTINCT ?d ?cognome ?nome ?legislatura ?gruppoParlamentare
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "male" ;foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
##gruppo parlamentari
?d ocd:rif_incarico ?incarico.
  ?incarico ocd:ruolo ?gruppoParlamentare. 


}  """
df_incarico_uomini_gp = get(endpoint, queryincaricodeputatiuominigp)
df_incarico_uomini_gp.to_csv("incaricouominigp.csv",  index=False, index_label=False)
print(len(df_incarico_uomini_gp))

queryincaricodeputatidonnegp = """
SELECT DISTINCT ?d ?cognome ?nome ?legislatura ?gruppoParlamentare
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
##gruppo parlamentari
?d ocd:rif_incarico ?incarico.
  ?incarico ocd:ruolo ?gruppoParlamentare. 


}  """
df_incarico_donne_gp = get(endpoint, queryincaricodeputatidonnegp)
df_incarico_donne_gp.to_csv("incaricodonnegp.csv",  index=False, index_label=False)
print(len(df_incarico_donne_gp)) 



queryincaricodeputatiuomini = """
SELECT DISTINCT ?d ?cognome ?nome ?gender ?legislatura
  ?ufficio ?ruolo
 
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender ?gender ;foaf:firstName ?nome.

## mandato
?mandato ocd:rif_elezione ?elezione.   
 
## uffici parlamentari
?d ocd:rif_ufficioParlamentare ?ufficioUri.
?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.
?d ocd:rif_incarico ?incarico.
  ?incarico ocd:ruolo ?ruolo. 

} """

df_incarico_uomini = get(endpoint, queryincaricodeputatiuomini)
df_incarico_uomini = df_incarico_uomini.drop_duplicates()
#df_female = df_incarico_totale[df_incarico_totale['genere'] == 'female']

# Filtra le righe con genere "male"
df_male = df_incarico_uomini[df_incarico_uomini['gender'] == 'male']

#df_incarico_finale = pd.concat([df_female, df_male])
#print(len(df_incarico_totale))
#print(df_incarico_totale)
#df_incarico_totale.to_csv("incaricodeputati.csv",  index=False, index_label=False)
