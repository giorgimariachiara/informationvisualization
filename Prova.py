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
df_incarico_uomini.to_csv("incaricouomini.csv",  index=False, index_label=False)

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
df_incarico_uomini_gp.to_csv("gpincaricouomini.csv",  index=False, index_label=False)


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
df_incarico_donne_gp.to_csv("gpincaricodonne.csv",  index=False, index_label=False)






#df_incarico_finale = pd.concat([df_female, df_male])
#print(len(df_incarico_totale))
#print(df_incarico_totale)
#df_incarico_totale.to_csv("incaricodeputati.csv",  index=False, index_label=False)
