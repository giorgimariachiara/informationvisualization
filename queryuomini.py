from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get
endpoint = "https://dati.camera.it/sparql"
pd.set_option('display.max_rows', None)
#QUERY NUMERO TOTALE UOMINI
querynumerototuomini = """
SELECT (COUNT(?nome) AS ?totale) where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatural. 
  ?legislatural dc:title ?legislatura. 
 }"""

dfnumerototuomini = sparql_dataframe.get(endpoint, querynumerototuomini)
#QUERY UOMINI ASSEMBLEA COSTITUENTE 
queryuomini0 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
  ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>.
 }
     
"""
dfmale0 = get(endpoint, queryuomini0)

#QUERY UOMINI LEGISLATURA 1  
queryuomini1 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>.
 }
  
"""

dfmale1 = get(endpoint, queryuomini1)

#QUERY UOMINI LEGISLATURA 2  
queryuomini2 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>.
 }
     
"""

dfmale2 = get(endpoint, queryuomini2)


#QUERY UOMINI LEGISLATURA 3  
queryuomini3 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>.
 }
     
"""

dfmale3 = get(endpoint, queryuomini3)

#QUERY UOMINI LEGISLATURA 4  
queryuomini4 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>.
 }
     
"""

dfmale4 = get(endpoint, queryuomini4)

#QUERY UOMINI LEGISLATURA 5  
queryuomini5 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>.
 }
     
"""

dfmale5 = get(endpoint, queryuomini5)

#QUERY UOMINI LEGISLATURA 6  
queryuomini6 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>.
 }
     
"""
dfmale6 = get(endpoint, queryuomini6)

#QUERY UOMINI LEGISLATURA 7 
queryuomini7 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>.
 }
     
"""

dfmale7 = get(endpoint, queryuomini7)

#QUERY UOMINI LEGISLATURA 8  
queryuomini8 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>.
 }
     
"""

dfmale8 = sparql_dataframe.get(endpoint, queryuomini8)

#QUERY UOMINI LEGISLATURA 9  
queryuomini9 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>.
 }
     
"""

dfmale9 = get(endpoint, queryuomini9)

#QUERY UOMINI LEGISLATURA 10 
queryuomini10 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>.
 }
     
"""

dfmale10 = get(endpoint, queryuomini10)

#QUERY UOMINI LEGISLATURA 11  
queryuomini11 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>.
 }
     
"""

dfmale11 = get(endpoint, queryuomini11)

#QUERY UOMINI LEGISLATURA 12  
queryuomini12 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>.
 }
     
"""

dfmale12 = get(endpoint, queryuomini12)

#QUERY UOMINI LEGISLATURA 13 
queryuomini13 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>.
 }
     
"""

dfmale13 = get(endpoint, queryuomini13)

#QUERY UOMINI LEGISLATURA 14 
queryuomini14 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>.
 }
     
"""

dfmale14 = get(endpoint, queryuomini14)

#QUERY UOMINI LEGISLATURA 15  
queryuomini15 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>.
 }
     
"""

dfmale15 = get(endpoint, queryuomini15)

#QUERY UOMINI LEGISLATURA 16  
queryuomini16 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>.
 }
"""

dfmale16 = get(endpoint, queryuomini16)


#QUERY UOMINI LEGISLATURA 17 
queryuomini17 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>.
 }
     
"""

dfmale17 = get(endpoint, queryuomini17)


#QUERY UOMINI LEGISLATURA 18 
queryuomini18 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>.
 }

     
"""

dfmale18 = get(endpoint, queryuomini18)


#QUERY UOMINI LEGISLATURA 19 
queryuomini19 = """
SELECT distinct ?persona ?nome ?cognome where {
  
  ?persona foaf:gender "male".
   ?persona rdf:type ocd:deputato.
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>.
 }
     
"""

dfmale19 = get(endpoint, queryuomini19)

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


#QUERY PRESIDENTI DEL CONSIGLIO

querypresidenticonsiglio = """
SELECT DISTINCT ?nome ?cognome where {
  ?legislatura ocd:rif_governo ?governo. 
  ?governo ocd:rif_presidenteConsiglioMinistri ?presidente. 
  ?presidente dc:title ?label. 
   ?presidente ocd:rif_persona ?persona. 
   ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona foaf:gender "male". 
  
   
 } """
dfpresidenticonsiglio = sparql_dataframe.get(endpoint, querypresidenticonsiglio)

#QUERY CONTO NUMERO PRESIDENTI CONSIGLIO 

querynumerocontopresidenticonsiglio = """SELECT (COUNT(*) AS ?NUMERO)
WHERE {
  { SELECT DISTINCT ?nome ?cognome WHERE {
   
  ?legislatura ocd:rif_governo ?governo. 
  ?governo ocd:rif_presidenteConsiglioMinistri ?presidente. 
  ?presidente dc:title ?label. 
   ?presidente ocd:rif_persona ?persona. 
   ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome .
  ?persona foaf:gender "male". } }} """

dfnumeropresidenti = sparql_dataframe.get(endpoint, querynumerocontopresidenticonsiglio)

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

merged_dfinal = merged_df.drop_duplicates()
#print(len(merged_dfinal))


dataframes = [dfmale0, dfmale1, dfmale2, dfmale3, dfmale4, dfmale5, dfmale6, dfmale7, dfmale8, dfmale9, dfmale10, dfmale11, dfmale12, dfmale13, dfmale14, dfmale15, dfmale16, dfmale17, dfmale18, dfmale19 ]

# creazione della lista per salvare i dataframe finali
new_dfs = []

# ciclo for per elaborare ogni dataframe nella lista
for df in dataframes:
    new_df = df.loc[:, ['nome', 'cognome']].drop_duplicates()
    new_dfs.append(new_df)

# unione di tutti i dataframe finali in un unico dataframe
merged_dataframe = pd.concat(new_dfs, axis=0)
merged_dataframe = merged_dataframe.drop_duplicates()

#merged_dataframe['nome_cognome'] = merged_dataframe['nome'] + ' ' + merged_dataframe['cognome']


uqery4 = """SELECT distinct ?nome ?cognome ?luogoNascita  where {
  
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome . 
  ?persona ocd:rif_mandatoCamera ?mandato. 
  ?mandato ocd:rif_leg ?legislatura.
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
             rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
 }"""

dataquery = get(endpoint, uqery4)


#duplicati = dataquery[dataquery.duplicated(['nome', 'cognome'], keep=False)]
#duplicati = duplicati.drop('luogoNascita', axis=1)
#duplicati = duplicati.drop_duplicates()

#ista_nomi_completi = duplicati['nome'].str.capitalize() + ' ' + duplicati['cognome']

# Conversione della lista in una lista di stringhe
#lista_nomi_completi = lista_nomi_completi.tolist()


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

#print(len(datauominiconlaurea)) #1669 uomini non laureati 382 di cui non abbiamo info e 3144
#duplicati = datalaurea[datalaurea.duplicated(['nome', 'cognome'], keep=False)]

queryprovaa ="""SELECT DISTINCT ?persona ?cognome ?nome ?info
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
dataprova = get(endpoint, queryprovaa)
dataprova = dataprova.drop_duplicates(["persona","nome", "cognome", "luogoNascita"])

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
df_nana = dataprova[dataprova['info'].isna()] 
df_nana = df_nana[["nome", "cognome", "luogoNascita"]]
nomi = df_nana['nome'] + ' ' + df_nana['cognome']
nomi = nomi.to_list()

# Stampa della lista di stringhe
#print(nomi)
print(uominilaureati)

