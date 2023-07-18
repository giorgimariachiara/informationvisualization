from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get
import re
endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)

#DONNE PER OGNI LEGISLATURA 
totale_donne_per_legislatura = """
SELECT DISTINCT ?persona ?cognome ?nome ?dataNascita ?luogoNascita "female" as ?gender ?legislatura
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
df_totale_donne_per_legislatura = get(endpoint, totale_donne_per_legislatura)
df_totale_donne_per_legislatura = df_totale_donne_per_legislatura[['nome', 'cognome', 'gender', 'legislatura']]

df_totale_donne_per_legislatura.to_csv("donneperlegislatura.csv",  index=False, index_label=False)

#QUERY UOMINI PER LEGISLATURA DA 0 A 10 
query_uomini1 = """
SELECT DISTINCT ?d ?nome ?cognome ?legislatura "male" as ?gender 
WHERE {
  ?d a ocd:deputato;
    ocd:rif_leg ?legislatura;
    ocd:rif_mandatoCamera ?mandato.
  OPTIONAL { ?d dc:description ?info }
  ?d foaf:surname ?cognome;
    foaf:gender "male";
    foaf:firstName ?nome.
  OPTIONAL {
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita;
      <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
      rdfs:label ?nato;
      ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
  }
  FILTER (?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/costituente> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>)
}

     
"""
dfmale_legislature1 = get(endpoint, query_uomini1)


#QUERY UOMINI LEGISLATURA DA 11 A 19 
query_uomini2 = """
SELECT DISTINCT ?d ?nome ?cognome ?legislatura "male" as ?gender 
WHERE {
  ?d a ocd:deputato;
    ocd:rif_leg ?legislatura;
    ocd:rif_mandatoCamera ?mandato.
  OPTIONAL { ?d dc:description ?info }
  ?d foaf:surname ?cognome;
    foaf:gender "male";
    foaf:firstName ?nome.
  OPTIONAL {
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita;
      <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
      rdfs:label ?nato;
      ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
  }
  FILTER (
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18> ||
          ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>
  )
}

"""

dfmale_legislature2 = get(endpoint, query_uomini2)

df_totale_uomini_per_legislatura = pd.concat([dfmale_legislature1, dfmale_legislature2])
df_totale_uomini_per_legislatura.to_csv("uominiperlegislatura.csv",  index=False, index_label=False)

df_totale_deputati_per_legislatura = pd.concat([df_totale_uomini_per_legislatura, df_totale_donne_per_legislatura])
df_totale_deputati_per_legislatura = df_totale_deputati_per_legislatura[["nome", "cognome", "gender", "legislatura"]]
df_totale_deputati_per_legislatura.to_csv("totaledeputatiperlegislatura.csv",  index=False, index_label=False)

import pandas as pd

def capitalize_name(name):
    parts = re.findall(r"[\w'-]+", name)
    capitalized_parts = [part.capitalize() for part in parts]
    return "_".join(capitalized_parts)

df_totale_deputati_per_legislatura['Persona'] =df_totale_deputati_per_legislatura[['nome', 'cognome']].apply(lambda x: '_'.join([capitalize_name(name) for name in x]), axis=1)

# Rimuovi le colonne 'nome' e 'cognome'
df_totale_deputati_per_legislatura2 =df_totale_deputati_per_legislatura.drop(['nome', 'cognome'], axis=1)

# Stampa il DataFrame aggiornato
#print(df_totale_deputati_per_legislatura2)
#df_totale_deputati_per_legislatura2.to_csv("deputatiperlegislatura.csv",  index=False, index_label=False)

#QUERY PER CITTà NASCITA 
uomini_nascita = """
SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?regione
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
?luogoNascitaUri ocd:parentADM3 ?regione. 
}}"""

df_uomini_nascita = get(endpoint, uomini_nascita)
df_uomini_nascita.rename(columns={"luogoNascita": "città"}, inplace=True)
df_uomini_nascita = df_uomini_nascita[["città", "regione"]]
print(len(df_uomini_nascita))
#df_uomini_nascita.to_csv("uominimappa.csv",  index=False, index_label=False)


#QUERY CARICA UOMINI 
querycaricauomini = """SELECT DISTINCT ?nome ?cognome ?ufficio ?organo where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome.
  ?persona foaf:surname ?cognome. 
  ?persona ocd:rif_ufficioParlamentare ?ufficioUri.
  ?ufficioUri ocd:rif_organo ?organoUri; ocd:carica ?ufficio.
  ?organoUri dc:title ?organo.
  
 } """

dfcaricauomini = sparql_dataframe.get(endpoint, querycaricauomini)

#QUERY GRUPPO PARLAMENTARE UOMINI ASSEMBLEA COSTITUENTE
querygruppoparuomini0 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini0 = sparql_dataframe.get(endpoint, querygruppoparuomini0)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 1 

querygruppoparuomini1 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini1 = sparql_dataframe.get(endpoint, querygruppoparuomini1)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 2 

querygruppoparuomini2 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini2 = sparql_dataframe.get(endpoint, querygruppoparuomini2)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 3

querygruppoparuomini3 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini3 = sparql_dataframe.get(endpoint, querygruppoparuomini3)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 4 

querygruppoparuomini4 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini4 = sparql_dataframe.get(endpoint, querygruppoparuomini4)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 5 

querygruppoparuomini5 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini5 = sparql_dataframe.get(endpoint, querygruppoparuomini5)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 6 

querygruppoparuomini6 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini7 = sparql_dataframe.get(endpoint, querygruppoparuomini6)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 7 

querygruppoparuomini7 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini7 = sparql_dataframe.get(endpoint, querygruppoparuomini7)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 8

querygruppoparuomini8 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini8 = sparql_dataframe.get(endpoint, querygruppoparuomini8)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 9 

querygruppoparuomini9 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini9 = sparql_dataframe.get(endpoint, querygruppoparuomini9)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 10 

querygruppoparuomini10 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini10 = sparql_dataframe.get(endpoint, querygruppoparuomini10)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 11 

querygruppoparuomini11 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini11 = sparql_dataframe.get(endpoint, querygruppoparuomini11)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 12 

querygruppoparuomini12 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini12 = sparql_dataframe.get(endpoint, querygruppoparuomini12)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 13 

querygruppoparuomini13 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini13 = sparql_dataframe.get(endpoint, querygruppoparuomini13)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 14 

querygruppoparuomini14 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini14 = sparql_dataframe.get(endpoint, querygruppoparuomini14)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 15 

querygruppoparuomini15 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini15 = sparql_dataframe.get(endpoint, querygruppoparuomini15)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 16 

querygruppoparuomini16 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini16 = sparql_dataframe.get(endpoint, querygruppoparuomini16)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 17 

querygruppoparuomini17 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini17 = sparql_dataframe.get(endpoint, querygruppoparuomini17)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 18 

querygruppoparuomini18 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini18 = sparql_dataframe.get(endpoint, querygruppoparuomini18)

#QUERY GRUPPO PARLAMENTARE LEGISLATURA 1 

querygruppoparuomini19 = """SELECT DISTINCT ?nome ?cognome ?gruppoPar where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>. 
  ?persona ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.} """

dfgruppoparuomini19 = sparql_dataframe.get(endpoint, querygruppoparuomini19)

#print(dfgruppoparuomini0)

#QUERY STUDI UOMO 

querystudiuomo ="""prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>

SELECT DISTINCT ?descrizione (COUNT(?descrizione) as ?numero) where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatural. 
  ?nome dc:description ?descrizione.  
 }
group by ?descrizione"""
dfstudiuomo = sparql_dataframe.get(endpoint, querystudiuomo)

new_df = dfmale0.loc[:, ['nome', 'cognome']]


new_df1 = dfmale1.loc[:, ['nome', 'cognome']]


new_df2 = dfmale2.loc[:, ['nome', 'cognome']]


new_df3 = dfmale3.loc[:, ['nome', 'cognome']]


new_df4 = dfmale4.loc[:, ['nome', 'cognome']]


new_df5 = dfmale5.loc[:, ['nome', 'cognome']]


new_df6 = dfmale6.loc[:, ['nome', 'cognome']]


new_df7 = dfmale7.loc[:, ['nome', 'cognome']]


new_df8 = dfmale8.loc[:, ['nome', 'cognome']]


new_df9 = dfmale9.loc[:, ['nome', 'cognome']]


new_df10 = dfmale10.loc[:, ['nome', 'cognome']]


new_df11 = dfmale11.loc[:, ['nome', 'cognome']]


new_df12 = dfmale12.loc[:, ['nome', 'cognome']]


new_df13= dfmale13.loc[:, ['nome', 'cognome']]


new_df14= dfmale14.loc[:, ['nome', 'cognome']]


new_df15= dfmale15.loc[:, ['nome', 'cognome']]


new_df16= dfmale16.loc[:, ['nome', 'cognome']]


new_df17= dfmale17.loc[:, ['nome', 'cognome']]


new_df18= dfmale18.loc[:, ['nome', 'cognome']]


new_df19= dfmale19.loc[:, ['nome', 'cognome']]

merged_df = pd.concat([new_df, new_df1, new_df2, new_df3, new_df4, new_df5, new_df6, new_df7, new_df8, new_df9, new_df10, new_df11, new_df12, new_df13, new_df14, new_df15, new_df16, new_df17, new_df18, new_df19], axis=0)

#merged_dfinal = merged_df.drop_duplicates()
#print(len(merged_df))


dataframes = [dfmale0, dfmale1, dfmale2, dfmale3, dfmale4, dfmale5, dfmale6, dfmale7, dfmale8, dfmale9, dfmale10, dfmale11, dfmale12, dfmale13, dfmale14, dfmale15, dfmale16, dfmale17, dfmale18, dfmale19 ]


