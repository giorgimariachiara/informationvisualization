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



#files  = "./violentOffense.csv"
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
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-0" (risultato: 202740)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-1" (risultato: 756901)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-2" (18048)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-3" (174754)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-4" (678119)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-5" (201593)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-6" (77082)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-7" (17747)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-8" (121539)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-9" (167613)
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-10" (38818) 
SELECT sum(Visitatori) FROM Periodi WHERE IDMuseo == "idmuseo-11" (5134)

SELECT COUNT(gender) FROM Nobel WHERE gender == "male" AND category ="http://data.nobelprize.org/terms/Physiology_or_Medicine"

DELETE FROM "FEMALE VIOLENT SEXUAL CRIME" WHERE UNODC== "06/06/2022";
DELETE FROM "FEMALE INTENTIONAL HOMICIDE" WHERE UNODC== "Iso3_code";
ALTER TABLE "VIOLENT OFFENSE"
RENAME COLUMN "Unnamed:12" TO Source;

ALTER TABLE "VIOLENT OFFENSE"
DROP COLUMN "Unnamed:10";
"""

#print("Musei_df_info:\n")
#print(Musei_df.info())
#print(Musei_df)


#print(Musei_df.info())
            
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
#QUERY TUTTE LE DONNE
endpoint = "https://dati.camera.it/sparql"

querydonne = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "female".
  ?nome ocd:rif_leg ?legislatura. 
 } ORDER BY ?legislatura
     
"""

df = sparql_dataframe.get(endpoint, querydonne)
print(df)

#QUERY UOMINI FINO ALLA 16 LEGISLATURA
queryuomini = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatura. 
 } ORDER BY ?legislatura
     
"""

df = sparql_dataframe.get(endpoint, queryuomini)
print(df)
#QUERY UOMINI FINO ALLA 17 LEGISLATURA
queryuomini = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_17>. 
 } ORDER BY ?legislatura
     
"""

df = sparql_dataframe.get(endpoint, queryuomini)
print(df)
#QUERY UOMINI FINO ALLA 18 LEGISLATURA
queryuomini = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_18>. 
 } ORDER BY ?legislatura
     
"""

df = sparql_dataframe.get(endpoint, queryuomini)
print(df)

#QUERY UOMINI FINO ALLA 19 LEGISLATURA
queryuomini = """
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix foaf:<http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?nome ?legislatura where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>. 
 } ORDER BY ?legislatura
     
"""

"""SELECT DISTINCT ?nome ?label where {
  
  ?nome foaf:gender "male".
  ?nome ocd:rif_leg ?legislatura.
  ?legislatura rdfs:label ?labelnome. 
  ?legislatura dc:title ?label. 
 } ORDER BY ?legislatura """

df = sparql_dataframe.get(endpoint, queryuomini)
print(df)

#QUERY luoghi di nascita città 

queryluoghinascita = """select distinct ?luogoNascital {
  ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
  ?nascita ocd:rif_luogo ?luogoNascitaUri.
  ?luogoNascitaUri rdfs:label ?luogoNascita.
  ?luogoNascitaUri dc:title ?luogoNascital.

        } 
     """