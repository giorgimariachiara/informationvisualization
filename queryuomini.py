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
#df_totale_donne_per_legislatura.to_csv("donneperlegislatura.csv",  index=False, index_label=False)

#QUERY UOMINI ASSEMBLEA COSTITUENTE 
query_uomini0 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/costituente" as ?legislatura where {
  
  ?persona foaf:gender "male".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>.
 }
     
"""
dfmale0 = get(endpoint, query_uomini0)


#QUERY UOMINI LEGISLATURA 1  
query_uomini1 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_01" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>.
 }
  
"""

dfmale1 = get(endpoint, query_uomini1)


#QUERY UOMINI LEGISLATURA 2  
query_uomini2 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_02" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>.
 }
     
"""

dfmale2 = get(endpoint, query_uomini2)


#QUERY UOMINI LEGISLATURA 3  
query_uomini3 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_03" as ?legislatura where {
  
  ?persona foaf:gender "male".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>.
 }
     
"""

dfmale3 = get(endpoint, query_uomini3)

#QUERY UOMINI LEGISLATURA 4  
query_uomini4 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_04" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>.
 }
     
"""

dfmale4 = get(endpoint, query_uomini4)

#QUERY UOMINI LEGISLATURA 5  
query_uomini5 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_05" as ?legislatura where {
  
  ?persona foaf:gender "male".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>.
 }
     
"""

dfmale5 = get(endpoint, query_uomini5)

#QUERY UOMINI LEGISLATURA 6  
query_uomini6 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_06" as ?legislatura where {
  
  ?persona foaf:gender "male".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>.
 }
     
"""
dfmale6 = get(endpoint, query_uomini6)

#QUERY UOMINI LEGISLATURA 7 
query_uomini7 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_07" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>.
 }
     
"""

dfmale7 = get(endpoint, query_uomini7)

#QUERY UOMINI LEGISLATURA 8  
query_uomini8 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_08" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>.
 }
     
"""

dfmale8 = sparql_dataframe.get(endpoint, query_uomini8)

#QUERY UOMINI LEGISLATURA 9  
query_uomini9 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_09" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>.
 }
     
"""

dfmale9 = get(endpoint, query_uomini9)

#QUERY UOMINI LEGISLATURA 10 
query_uomini10 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_10" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>.
 }
     
"""

dfmale10 = get(endpoint, query_uomini10)

#QUERY UOMINI LEGISLATURA 11  
query_uomini11 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_11" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>.
 }
     
"""

dfmale11 = get(endpoint, query_uomini11)

#QUERY UOMINI LEGISLATURA 12  
query_uomini12 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_12" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>.
 }
     
"""

dfmale12 = get(endpoint, query_uomini12)

#QUERY UOMINI LEGISLATURA 13 
query_uomini13 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_13" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>.
 }
     
"""

dfmale13 = get(endpoint, query_uomini13)

#QUERY UOMINI LEGISLATURA 14 
query_uomini14 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_14" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>.
 }
     
"""

dfmale14 = get(endpoint, query_uomini14)

#QUERY UOMINI LEGISLATURA 15  
query_uomini15 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_15" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>.
 }
     
"""

dfmale15 = get(endpoint, query_uomini15)

#QUERY UOMINI LEGISLATURA 16  
query_uomini16 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_16" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>.
 }
"""

dfmale16 = get(endpoint, query_uomini16)


#QUERY UOMINI LEGISLATURA 17 
query_uomini17 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_17" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>.
 }
     
"""

dfmale17 = get(endpoint, query_uomini17)


#QUERY UOMINI LEGISLATURA 18 
query_uomini18 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_18" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>.
 }
 
"""

dfmale18 = get(endpoint, query_uomini18)


#QUERY UOMINI LEGISLATURA 19 
query_uomini19 = """
SELECT distinct ?persona ?nome ?cognome "http://dati.camera.it/ocd/legislatura.rdf/repubblica_19" as ?legislatura where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>.
 }
     
"""

dfmale19 = get(endpoint, query_uomini19)

totale_uomini_per_legislatura = pd.concat([dfmale0, dfmale1, dfmale2, dfmale3, dfmale4, dfmale5, dfmale6, dfmale7, dfmale8, dfmale9, dfmale10, dfmale11, dfmale12, dfmale13, dfmale14, dfmale15, dfmale16, dfmale17, dfmale18, dfmale19], axis=0)
df_totale_uomini_per_legislatura = totale_uomini_per_legislatura.assign(gender="male")
df_totale_uomini_per_legislatura = df_totale_uomini_per_legislatura[["nome", "cognome", "gender", "legislatura"]]
df_totale_deputati_per_legislatura = pd.concat([df_totale_uomini_per_legislatura, df_totale_donne_per_legislatura])
#df_totale_deputati_per_legislatura.to_csv("totaledeputatiperlegislatura.csv",  index=False, index_label=False)

import pandas as pd

def capitalize_name(name):
    parts = re.findall(r"[\w'-]+", name)
    capitalized_parts = [part.capitalize() for part in parts]
    return "_".join(capitalized_parts)

df_totale_deputati_per_legislatura['Persona'] =df_totale_deputati_per_legislatura[['nome', 'cognome']].apply(lambda x: '_'.join([capitalize_name(name) for name in x]), axis=1)

# Rimuovi le colonne 'nome' e 'cognome'
df_totale_deputati_per_legislatura2 =df_totale_deputati_per_legislatura.drop(['nome', 'cognome'], axis=1)

# Stampa il DataFrame aggiornato
print(df_totale_deputati_per_legislatura2)
df_totale_deputati_per_legislatura2.to_csv("deputatiperlegislatura.csv",  index=False, index_label=False)
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

print(dfgruppoparuomini0)

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

#CONTO TUTTI UOMINI CON MERGE DATAFRAME DI OGNI LEGISLATURA 

#merged_df = pd.merge(dfmale0, dfmale1, on=['nome', 'cognome'])
#final_df = merged_df.drop_duplicates(subset=['nome', 'cognome'])

#merged_df = pd.merge(merged_df, df3, on='key', how='outer')
#print(dfmale1)
#final_df = dfmale1.drop_duplicates(subset=['nome', 'cognome'], keep=False)

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

#query laurea 

querylaureauominitutti = """SELECT distinct ?nome ?cognome ?descrizione ?luogoNascita where {
  
  ?persona foaf:gender "female".
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
datalaurea = get(endpoint, querylaureauominitutti)

datalaurea = datalaurea.sort_values(by='cognome')
df_nan = datalaurea[datalaurea['descrizione'].isna()] #382 persone non hanno la descrizione
queryuominisenzalaurea = """SELECT distinct ?nome ?cognome ?descrizione ?luogoNascita where {
  
  ?persona foaf:gender "male".
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
  FILTER regex(?descrizione, "^(?!.*Laurea|laurea)")
  
 }"""


datanonlaureauomini =get(endpoint, queryuominisenzalaurea)


#duplicati = datalaurea[datalaurea.duplicated(['nome', 'cognome'], keep=False)]

querydefinitivalaureauomini ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
?dataNascita ?luogoNascita 
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
dataprova = get(endpoint, querydefinitivalaureauomini)
dataprova = dataprova.drop_duplicates(["persona","nome", "cognome", "luogoNascita"])
print(len(dataprova))
df_nana = dataprova[dataprova['info'].isna()] 
dataprova['info'] = dataprova['info'].fillna('')
mask = dataprova['info'].str.contains('Laurea|laurea|Master|LAUREA')

# Estrarre le righe che soddisfano la maschera
laureati = dataprova[mask]

laureati = laureati.assign(info="yes")
laureati = laureati.assign(gender='male')
laureati = laureati[["info", "gender"]]
uominilaureati = laureati.rename(columns={'info': 'graduated'})
maskuomininonlaureati =~dataprova['info'].str.contains('Laurea|laurea|Master|LAUREA', na=False) & dataprova['info'].ne('')
uomininonlaureati = dataprova[maskuomininonlaureati]
uomininonlaureati = uomininonlaureati.assign(info="no")
uomininonlaureati = uomininonlaureati.assign(gender='male')
uomininonlaureati = uomininonlaureati[["info", "gender"]]
uomininonlaureati = uomininonlaureati.rename(columns={'info': 'graduated'})

#uominilaureacsv = pd.concat([uomininonlaureati, uominilaureati],  axis=0)
#uominilaureacsv.to_csv("mengraduation.csv",  index=False, index_label=False)
#print(len(uominilaureacsv)) #senza info sono 162, 3293 si, 1749

df_nana = df_nana[["nome", "cognome", "luogoNascita"]]
nomi = df_nana['nome'] + ' ' + df_nana['cognome']
nomi = nomi.to_list()

#QUERY PER TROVARE PARTITO 
query_partito_uomini1 = """SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?gruppoPar
WHERE {
  ?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
  
  ?d a ocd:deputato;
     ocd:rif_leg ?legislatura;
     ocd:rif_mandatoCamera ?mandato.
  ?d ocd:aderisce ?gruppo.
  ?gruppo rdfs:label ?gruppoPar.
  
  ## Anagrafica
  ?d foaf:surname ?cognome; foaf:gender "male"; foaf:firstName ?nome.
  
  OPTIONAL {
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
    rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
  }
  
  FILTER (?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/costituente>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>)
}
"""

query_partito_uomini2 = """SELECT DISTINCT ?persona ?cognome ?nome ?luogoNascita ?gruppoPar
WHERE {
  ?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.
  
  ?d a ocd:deputato;
     ocd:rif_leg ?legislatura;
     ocd:rif_mandatoCamera ?mandato.
  ?d ocd:aderisce ?gruppo.
  ?gruppo rdfs:label ?gruppoPar.
  
  ## Anagrafica
  ?d foaf:surname ?cognome; foaf:gender "male"; foaf:firstName ?nome.
  
  OPTIONAL {
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
    rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
  }
  
  FILTER (
             ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>
          || ?legislatura = <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>)
}
"""
query_partito_donne = """SELECT DISTINCT ?persona ?cognome ?nome
?luogoNascita ?gruppoPar
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

?d a ocd:deputato; 
ocd:rif_leg ?legislatura;
ocd:rif_mandatoCamera ?mandato.
OPTIONAL{?d dc:description ?info}
?d ocd:aderisce ?gruppo . 
  ?gruppo rdfs:label ?gruppoPar.

##anagrafica
?d foaf:surname ?cognome; foaf:gender "female" ;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
?luogoNascitaUri dc:title ?luogoNascita.

}}"""
#dataprova1 = dataprova[["nome", "cognome"]]
df_partito_donne = get(endpoint, query_partito_donne)
df_partito_donne['gruppoPar'] = df_partito_donne['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_donne = df_partito_donne.drop_duplicates(["persona","nome", "cognome", "luogoNascita", "gruppoPar"])
df_partito_donne = df_partito_donne[["nome", "cognome", "gruppoPar"]]
df_partito_donne = df_partito_donne[["gruppoPar"]]
df_partito_donne.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_donne = df_partito_donne.assign(gender='female')
#print(len(df_partito_donne))

df_partito_uomini1 = get(endpoint, query_partito_uomini1)
df_partito_uomini1['gruppoPar'] = df_partito_uomini1['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_uomini1 = df_partito_uomini1.drop_duplicates(["persona","nome", "cognome", "luogoNascita", "gruppoPar"])
df_partito_uomini1 = df_partito_uomini1[["gruppoPar"]]
df_partito_uomini1.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_uomini1 = df_partito_uomini1.assign(gender='male')

df_partito_uomini2 = get(endpoint, query_partito_uomini2)
df_partito_uomini2['gruppoPar'] = df_partito_uomini2['gruppoPar'].str.extract(r'^(.*?) \(')
df_partito_uomini2 = df_partito_uomini2.drop_duplicates(["persona","nome", "cognome", "luogoNascita", "gruppoPar"])
df_partito_uomini2 = df_partito_uomini2[["gruppoPar"]]
df_partito_uomini2.rename(columns={"gruppoPar": "partito"}, inplace=True)
df_partito_uomini2 = df_partito_uomini2.assign(gender='male')

df_partito_totale = pd.concat([df_partito_uomini1, df_partito_uomini2, df_partito_donne])
#df_partito_totale.to_csv("partytotal.csv",  index=False, index_label=False)
#print(len(df_partito_totale))

lista_partiti = df_partito_totale[["partito"]].drop_duplicates()
lista_partiti = lista_partiti.values.flatten().tolist()
#print(len(lista_partiti))

length = len(lista_partiti)

part_size = length // 8

parte_1 = lista_partiti[:part_size]
parte_2 = lista_partiti[part_size:2*part_size]
parte_3 = lista_partiti[2*part_size:3*part_size]
parte_4 = lista_partiti[3*part_size:4*part_size]
parte_5 = lista_partiti[4*part_size:5*part_size]
parte_6 = lista_partiti[5*part_size:6*part_size]
parte_7 = lista_partiti[6*part_size:7*part_size]
parte_8 = lista_partiti[7*part_size:]

# Gestione dei casi in cui la lunghezza della lista non è divisibile in modo uniforme
if length % 8 != 0:
    parte_8 += lista_partiti[8*part_size:]

# Stampa delle parti
#print("Parte 1:", len(parte_1))
"""
print("Parte 2:", parte_2)
print("Parte 3:", parte_3)
print("Parte 4:", parte_4)
print("Parte 5:", parte_5)
print("Parte 6:", parte_6)
print("Parte 7:", parte_7)
print("Parte 8:", parte_8)
"""
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import time

"""
def getdatafromwiki(parties):
    data_list = []

    for party in parties:
        query = '''
        SELECT distinct ?party ?partyLabel ?alignment ?al WHERE {
          ?party wdt:P31 wd:Q7278;
                 rdfs:label ?partyLabel;
                 wdt:P17 wd:Q38;
                 wdt:P1387 ?alignment.
          ?alignment rdfs:label ?al. 
          FILTER(LANG(?partyLabel) = "it" && CONTAINS(LCASE(?partyLabel), "''' + party.lower() + '''")).
          FILTER(LANG(?al) = "it")
        }
        '''
        url = 'https://query.wikidata.org/sparql'
        try:
            r = requests.get(url, params={'format': 'json', 'query': query})
            r.raise_for_status()
            data = r.json()
            bindings = data['results']['bindings']

            if len(bindings) == 0:
                data_list.append({'Party': party, 'Alignment': ''})
            else:
                for binding in bindings:
                    party_label = binding['partyLabel']['value']
                    alignment = binding['al']['value']
                    data_list.append({'Party': party_label, 'Alignment': alignment})
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError, requests.exceptions.JSONDecodeError) as e:
            print(f"Error occurred for party: {party}. Error message: {str(e)}")

        time.sleep(1)
    df = pd.DataFrame(data_list)
    return df


# Esempio di utilizzo
df = getdatafromwiki(lista_partiti)
print(lista_partiti)
#df2 = getdatafromwiki(parte_2)
#df3 = getdatafromwiki(parte_3)
#dff = pd.concat([df, df2, df3])

#print(df)

"""

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
da = get_uri_from_names(nomi)
da = da.drop_duplicates(["persona", "nome", "cognome"])
"""
