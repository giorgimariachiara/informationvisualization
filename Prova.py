import csv
from logging import raiseExceptions
import pandas as pd
from json import load
from pandas import DataFrame
import os.path
from sqlite3 import connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

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
    print(Periodi_DF)

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