
from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe
import matplotlib.pyplot as plt

endpoint = "https://dati.camera.it/sparql"

#QUERY GRUPPO PARLAMENTARE DONNE 
querygruppopardonne = """SELECT DISTINCT ?nome ?cognome (CONCAT(STRBEFORE(?gruppoPar, " ("), STRAFTER(?gruppoPar, ")"))) AS ?gruppo
WHERE {
  ?persona foaf:gender "female".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona ocd:aderisce ?gruppo. 
  ?gruppo rdfs:label ?gruppoPar.
  FILTER regex(?gruppoPar, "\\\\(.*\\\\)")
}
 """

querygruppoparuomini = """SELECT DISTINCT ?nome ?cognome (CONCAT(STRBEFORE(?gruppoPar, " ("), STRAFTER(?gruppoPar, ")"))) AS ?gruppo
WHERE {
  ?persona foaf:gender "male".
  ?persona foaf:firstName ?nome. 
  ?persona foaf:surname ?cognome. 
  ?persona ocd:aderisce ?gruppo. 
  ?gruppo rdfs:label ?gruppoPar.
  FILTER regex(?gruppoPar, "\\\\(.*\\\\)")
}
 """
#per farla su virtuoso "\\(.*\\)" tocca usare questo che su python non viene preso daje tuttaaaa
dfgruppoparuomini = sparql_dataframe.get(endpoint, querygruppoparuomini)

dfgruppopardonne = sparql_dataframe.get(endpoint, querygruppopardonne)

count = len(dfgruppopardonne[['nome', 'cognome']].drop_duplicates())
counts = dfgruppopardonne['gruppo'].value_counts()

counts_dict = {}
for index, row in dfgruppopardonne.iterrows():
    gruppo = row["gruppo"]
    if gruppo in counts_dict:
        counts_dict[gruppo] += 1
    else:
        counts_dict[gruppo] = 1


plt.bar(range(len(counts_dict)), list(counts_dict.values()), align='center')
plt.xticks(range(len(counts_dict)), list(counts_dict.keys()), rotation='vertical')
plt.xlabel('Gruppo Par')
plt.ylabel('Number of people')
plt.title('Population by Gruppo Par')
plt.show()

print(dfgruppoparuomini)