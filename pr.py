from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe 
import pandas as pd 
from sparql_dataframe import get
import requests
from bs4 import BeautifulSoup
import requests
import wikipediaapi
import re

def extract_birth_date(text):
    matches = re.findall(r'\d+\s+\w+\s+\d+', text)
    if matches:
        birth_date = matches[0]
        return birth_date.strip()
    return None

def find_wikipedia_url(nome_cognome, data_nascita):
    wiki_wiki = wikipediaapi.Wikipedia('it')

    url_politico_data = None
    url_politico = None
    url_generale = None

    for anno_iniziale in [1952, 1940, 1929]:
        page_name = f"{nome_cognome} (politico {anno_iniziale})"
        page = wiki_wiki.page(page_name)
        if page.exists():
            content = page.text
            birth_date = extract_birth_date(content)
            if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                url_politico_data = page.fullurl
                break

    if not url_politico_data:
        page_name = f"{nome_cognome} (politico)"
        page = wiki_wiki.page(page_name)
        if page.exists():
            content = page.text
            birth_date = extract_birth_date(content)
            if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                url_politico = page.fullurl

    if not url_politico_data and not url_politico:
        page_name = nome_cognome
        page = wiki_wiki.page(page_name)
        if page.exists():
            content = page.text
            birth_date = extract_birth_date(content)
            if birth_date and (birth_date == data_nascita or birth_date.split(' – ')[0] == data_nascita):
                url_generale = page.fullurl

    if url_politico_data:
        return f"URL della pagina di Wikipedia per {nome_cognome} (politico con data): {url_politico_data}"
    elif url_politico:
        return f"URL della pagina di Wikipedia per {nome_cognome} (politico): {url_politico}"
    elif url_generale:
        return f"URL della pagina di Wikipedia generica per {nome_cognome}: {url_generale}"
    else:
        return f"Nessun URL trovato per {nome_cognome}"

# Esempio di utilizzo con la lista di tuple come input
input_list = [('Giuseppe_Sales', '6 febbraio 1901'), ('Carlo_Petrone', '11 agosto 1899'), ('Giuseppe_Anzilotti', '26 dicembre 1919'), ('Filippo_Di_Filippo', '23 aprile 1887'), ('Giovanni_Mantovani', '1 gennaio 1888'), ('Aldo_Gregorelli', '24 luglio 1937'), ('Gaetano_Zanotti', '1 dicembre 1893'), ('Ludovico_Boetti_Villanis', '11 febbraio 1931'), ('Domenico_Pettini', '9 dicembre 1895'), ('Giorgio_Spadaccia', '19 luglio 1941'), ('Mario_Brancaccio', '30 gennaio 1933'), ('Arnaldo_Brunetto', '29 aprile 1941'), ('Giovanni_Battista_Bruni', '28 maggio 1926'), ('Alessandro_Costa', '6 febbraio 1929'), ("Guido_D'Angelo", '22 aprile 1933'), ('Giuseppe_Lavorato', '31 gennaio 1938'), ('Giuseppe_Lucenti', '30 aprile 1945'), ('Paolo_Martuscelli', '9 maggio 1924'), ('Giovanni_Vernetti', '27 novembre 1960'), ('Massimo_Tedeschi', '10 dicembre 1951'), ('Lorenzo_Emilio_Ria', '17 maggio 1954'), ('Andrea_Mitolo', '16 aprile 1914'), ('Domenico_Modugno', '9 gennaio 1928'), ('Massimo_Pacetti', '2 agosto 1940'), ('Giulio_Quercini', '16 dicembre 1941'), ('Franco_Ricci', '14 febbraio 1948'), ('Lorenzo_Dellai', '28 novembre 1959'), ('Giacomo_Antonio_Schettini', '3 agosto 1934'), ('Alberto_Volponi', '28 febbraio 1947'), ('Siro_Castrucci', '1 maggio 1930'), ('Giovanni_Piccirillo', '15 novembre 1946'), ('Alberto_Sinatra', '31 agosto 1933'), ('Gaetano_Azzolina', '29 maggio 1931'), ('Francesco_Spina', '31 agosto 1930'), ('Diego_Senter', '4 marzo 1954'), ('Mario_De_Cristofaro', ''), ('Andrea_Vecchio', '14 settembre 1939'), ('Erasmo_Palazzotto', '19 novembre 1982'), ('Vincenzino_Culicchia', '9 ottobre 1932'), ('Antonio_Miceli', '14 settembre 1940'), ('Salvatore_Margiotta', '20 aprile 1954'), ('Paolo_Franzini_Tibaldeo', '18 luglio 1951'), ('Ferdinando_De_Franciscis', '1 settembre 1930'), ('Luigi_Colonna', '1 febbraio 1927'), ('Mauro_Fabris', '14 marzo 1958'), ('Luciano_Donner', '13 febbraio 1941'), ('Giovanni_Battista_Melis', '27 marzo 1922'), ('Federico_Freni', '1 luglio 1980'), ('Giovanni_Donzelli', '28 novembre 1975'), ('Mauro_Sutto', '1 gennaio 1980'), ('Mauro_Malaguti', '1 marzo 1960')]

for item in input_list:
    nome_cognome, data_nascita = item
    result = find_wikipedia_url(nome_cognome, data_nascita)
    print(result)
