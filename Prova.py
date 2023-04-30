import csv
from logging import raiseExceptions
import pandas as pd
from json import load
from pandas import DataFrame
import os.path
from sqlite3 import connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import concat, read_sql
import csv
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import sys
import time


from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe

"""
if os.path.exists(csv_path):
    Musei_df = pd.read_csv(csv_path, delimiter=";", keep_default_na=False, dtype={
        "anno": "string",
        "mese": "string",
        "museo": "string",
        "visitatori": "string",
        "utenti": "string",
    }, encoding="utf-8")
    
    Musei_DF=  Musei_df[['Museo']].drop_duplicates()
    Musei_DF.insert(0, 'id', range(0, Musei_DF.shape[0]))
    Musei_DF['id'] = Musei_DF['id'].apply(lambda x: 'idmuseo-' + str(int(x)))

    Anni = Musei_df[["Anno"]].drop_duplicates() 
    Anni.insert(0, 'id', range(0, Anni.shape[0]))
    Anni['id'] = Anni['id'].apply(lambda x: 'idanno-' + str(int(x)))

    Periodi_df = Musei_df[["Anno", "Mese", "Museo", "Visitatori"]]
    Periodi_df = Periodi_df.merge(Musei_DF, on="Museo").drop_duplicates()
    Periodi_df = Periodi_df.merge(Anni, on="Anno")
    Periodi_DF = Periodi_df[["id_x", "id_y", "Mese", "Visitatori"]]
    Periodi_DF = Periodi_DF.rename(columns={"id_x" : "IDMuseo", "id_y" : "IDAnno"})
    #print(Periodi_DF)

    with connect(db_path) as con:
                Musei_DF.to_sql(
                    "Musei", con, if_exists="replace", index=False)
                Anni.to_sql(
                    "Anni", con, if_exists="replace", index=False)
                Periodi_DF.to_sql(
                    "Periodi", con, if_exists="replace", index=False)

                con.commit()
    
else:
    raise Exception(f"CSV file '{csv_path}' does not exist!") 
"""

"""
def getSumVisitors(idmuseo):
    for museo in idmuseo: 
        list = []
        if type(idmuseo) == str:
            with connect(db_path) as con:
                SQL = "SELECT SUM(A.Visitatori), B.Museo FROM Periodi AS A JOIN Musei AS B ON A.IDMuseo == B.id WHERE A.IDMuseo == '" + idmuseo + "';"
                co = list.append(read_sql(SQL, con))
            return co   
        

        else:
            raise TypeError("The input parameter publicationYear is not an integer!")
"""
"""

def SumVisitors(listOfMuseums):
        for el in listOfMuseums:
                with connect(db_path) as con:
                    museums = pd.DataFrame()
                    for idmuseo in listOfMuseums:
                        SQL = read_sql("SELECT SUM(A.Visitatori), B.Museo FROM Periodi AS A JOIN Musei AS B ON A.IDMuseo == B.id WHERE A.IDMuseo == '" + idmuseo + "';", con)
                        museums = concat([museums, SQL]) 
                return  museums
       
list = ["idmuseo-0","idmuseo-1", "idmuseo-2","idmuseo-3", "idmuseo-4","idmuseo-5", "idmuseo-6", "idmuseo-7", "idmuseo-8", "idmuseo-9", "idmuseo-10", "idmuseo-11"]
risultato = SumVisitors(list)
#print(risultato)
#print(risultato.to_csv("visitors.csv", index= False))
#print(Periodi_DF.plot(kind='bar'))
#variabile = SumVisitors(list)
#my_plot = sns(variabile)
#sns.displot(variabile, x="anni", binwidth=3)
"""
"""


SELECT COUNT(gender) FROM Nobel WHERE gender == "male" AND category ="http://data.nobelprize.org/terms/Physiology_or_Medicine"

DELETE FROM "FEMALE VIOLENT SEXUAL CRIME" WHERE UNODC== "06/06/2022";
DELETE FROM "FEMALE INTENTIONAL HOMICIDE" WHERE UNODC== "Iso3_code";
ALTER TABLE "VIOLENT OFFENSE"
RENAME COLUMN "Unnamed:12" TO Source;

ALTER TABLE "VIOLENT OFFENSE"
DROP COLUMN "Unnamed:10";
"""

            
"""
from pandas import read_csv
publicationDF_CSV = read_csv(csv, keep_default_na=False)
print("publicationDF_CSV_info:\n")
print(Musei_df.info())
#print(DataCSV(csv))
for chunk in pd.read_csv("sample.txt", chunksize=2):
    print(chunk)
    print("-----")
"""
"""
def creadatabase(file):
    lista = list() 
    for chunk in pd.read_csv(file ,sep=';', chunksize=9909):
            lista.append(chunk)
            print(lista)

variabile = creadatabase(files)
print(len(variabile))
#print(creadatabase(files))
"""
"""
            with connect(db_path) as con:
                            chunk.to_sql(
                            "violent", con, if_exists="replace", index=False)
                            con.commit() 
"""

"""
files         = ".\HDI.csv"
csv_database = create_engine('sqlite:///HDI.db', echo=False)

df = pd.read_csv(files, sep=";", dtype='unicode' )

start = time.time()
chunksize = 100000
i = 0
j = 1
for df in pd.read_csv(files, chunksize=chunksize, iterator=True, sep=";", dtype='unicode'):
    df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
df.index += j
i+=1
df.to_sql('HDI Trends', csv_database, if_exists='append')
j = df.index[-1] + 1
end = time.time()
print(end - start)

"""
"""
import rdflib.graph as g 
graph = g.Graph()
graph.parse('persona.rdf', format='rdf')

print(graph.serialize(format='pretty-xml'))
"""

endpoint = "https://dati.camera.it/sparql"

#QUERY TUTTE LE DONNE

querydonne = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatura. 
 } ORDER BY ?legislatura
     
"""

dffemale = sparql_dataframe.get(endpoint, querydonne)


#QUERY UOMINI ASSEMBLEA COSTITUENTE 
queryuomini0 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>. 
 } 
     
"""

dfmale0 = sparql_dataframe.get(endpoint, queryuomini0)

#QUERY UOMINI LEGISLATURA 1  
queryuomini1 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_01>. 
 } 
     
"""

dfmale1 = sparql_dataframe.get(endpoint, queryuomini1)

#QUERY UOMINI LEGISLATURA 2  
queryuomini2 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_02>. 
 } 
     
"""

dfmale2 = sparql_dataframe.get(endpoint, queryuomini2)


#QUERY UOMINI LEGISLATURA 3  
queryuomini3 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_03>. 
 } 
     
"""

dfmale3 = sparql_dataframe.get(endpoint, queryuomini3)

#QUERY UOMINI LEGISLATURA 4  
queryuomini4 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_04>. 
 } 
     
"""

dfmale4 = sparql_dataframe.get(endpoint, queryuomini4)

#QUERY UOMINI LEGISLATURA 5  
queryuomini5 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_05>. 
 } 
     
"""

dfmale5 = sparql_dataframe.get(endpoint, queryuomini5)

#QUERY UOMINI LEGISLATURA 6  
queryuomini6 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_06>. 
 } 
     
"""

dfmale6 = sparql_dataframe.get(endpoint, queryuomini6)


#QUERY UOMINI LEGISLATURA 7 
queryuomini7 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_07>. 
 } 
     
"""

dfmale7 = sparql_dataframe.get(endpoint, queryuomini7)

#QUERY UOMINI LEGISLATURA 8  
queryuomini8 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_08>. 
 } 
     
"""

dfmale8 = sparql_dataframe.get(endpoint, queryuomini8)

#QUERY UOMINI LEGISLATURA 9  
queryuomini9 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_09>. 
 } 
     
"""

dfmale9 = sparql_dataframe.get(endpoint, queryuomini9)

#QUERY UOMINI LEGISLATURA 10 
queryuomini10 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10>. 
 } 
     
"""

dfmale10 = sparql_dataframe.get(endpoint, queryuomini10)

#QUERY UOMINI LEGISLATURA 11  
queryuomini11 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_11>. 
 } 
     
"""

dfmale11 = sparql_dataframe.get(endpoint, queryuomini11)

#QUERY UOMINI LEGISLATURA 12  
queryuomini12 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_12>. 
 } 
     
"""

dfmale12 = sparql_dataframe.get(endpoint, queryuomini12)

#QUERY UOMINI LEGISLATURA 13 
queryuomini13 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_13>. 
 } 
     
"""

dfmale13 = sparql_dataframe.get(endpoint, queryuomini13)

#QUERY UOMINI LEGISLATURA 14 
queryuomini14 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_14>. 
 } 
     
"""

dfmale14 = sparql_dataframe.get(endpoint, queryuomini14)

#QUERY UOMINI LEGISLATURA 15  
queryuomini15 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_15>. 
 } 
     
"""

dfmale15 = sparql_dataframe.get(endpoint, queryuomini15)

#QUERY UOMINI LEGISLATURA 16  
queryuomini16 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_16>. 
 } 
     
"""

dfmale16 = sparql_dataframe.get(endpoint, queryuomini16)


#QUERY UOMINI LEGISLATURA 17 
queryuomini17 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>. 
 } 
     
"""

dfmale17 = sparql_dataframe.get(endpoint, queryuomini17)


#QUERY UOMINI LEGISLATURA 18 
queryuomini18 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>. 
 } 

     
"""

dfmale18 = sparql_dataframe.get(endpoint, queryuomini18)


#QUERY UOMINI LEGISLATURA 19 
queryuomini19 = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>. 
 } 
     
"""

"""SELECT DISTINCT ?nome ?label where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatura.
  ?legislatura rdfs:label ?labelnome. 
  ?legislatura dc:title ?label. 
 } ORDER BY ?legislatura """

dfmale19 = sparql_dataframe.get(endpoint, queryuomini19)


#QUERY LUOGHI NASCITA 

querycittànascita = """select distinct ?luogoNascital {
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?luogoNascital.

        } 
     """
dfciitànascita = sparql_dataframe.get(endpoint, querycittànascita)

queryregioninascita = """
    select ?regione ?persona{
    ?persona foaf:gender "female".
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri ocd:parentADM3 ?regione . 
    
    } """


dfregioninascita = sparql_dataframe.get(endpoint, queryregioninascita)


queryprova = """    select distinct ?persona  {
    
    ?persona ocd:rif_leg ?legislatura.
    
    
    } ORDER BY ?legislatura

"""

dftr = sparql_dataframe.get(endpoint, queryprova)
 


QUERYSOMMATUTTI = """SELECT DISTINCT ?nome ?legislatura where {{
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatura. 
 } 
UNION 
{?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatura. 
  }}ORDER BY ?legislatura"""


QUERYSOMMATUTTI0 = """SELECT DISTINCT ?nome where {{
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>. 
 } 
UNION 
{?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/costituente>. 
  }}"""


dfsommmatutti = sparql_dataframe.get(endpoint, QUERYSOMMATUTTI)

dfsommmatutti0 = sparql_dataframe.get(endpoint, QUERYSOMMATUTTI0)

print(dfsommmatutti0)

