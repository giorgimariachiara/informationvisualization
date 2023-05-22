from bs4 import BeautifulSoup
import requests 


# Lista di nomi e cognomi delle persone
persone = ["Grazia_Sestini", "Nome_Cognome", "Altro_Nome_Cognome"]

for persona in persone:
    # Costruisci l'URL della pagina di Wikipedia per la persona corrente
    url = f"https://it.wikipedia.org/wiki/{persona}"

    # Effettua la richiesta HTTP alla pagina di Wikipedia
    response = requests.get(url)
    if response.status_code == 200:
        # Parsa l'HTML della pagina con Beautiful Soup
        soup = BeautifulSoup(response.content, "html.parser")

        # Esegui l'elaborazione dei dati o l'estrazione delle informazioni necessarie
        # Ad esempio, puoi trovare il titolo della pagina e stamparlo
        titolo_pagina = soup.find("h1", class_="firstHeading").text
        print(f"Pagina di Wikipedia per {persona}: {titolo_pagina}")
    else:
        print(f"Errore nella richiesta della pagina di Wikipedia per {persona}")
url = "https://it.wikipedia.org/wiki/Grazia_Sestini"
response = requests.get(url)
html_content = response.text

soup = BeautifulSoup(html_content, "html.parser")
biografia_header = soup.find(id="Biografia")
if biografia_header:
    # Navigate to the parent section and extract the information
    biografia_section = biografia_header.find_parent("div", class_="mw-parser-output")
    if biografia_section:
        # Extract the text or do further processing
        biografia_text = biografia_section.get_text(strip=True)
        print(biografia_text)
else:
    print("Biografia header not found.") 