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
csv_path = "./presenze_musei.csv"
db_path = "musei.db"

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
print(risultato)
#print(risultato.to_csv("visitors.csv", index= False))
#print(Periodi_DF.plot(kind='bar'))
#variabile = SumVisitors(list)
#my_plot = sns(variabile)
#sns.displot(variabile, x="anni", binwidth=3)

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
"""