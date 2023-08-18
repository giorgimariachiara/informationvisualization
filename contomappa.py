import pandas as pd

# Carica i dati dal file CSV
df = pd.read_csv('uominimappa.csv')

# Conta il numero di uomini per regione
regione_counts = df[df['regione'].notnull()].groupby('regione')['città'].count()

# Crea un DataFrame per le regioni italiane
regioni_italiane = pd.Series(range(1, 21), index=[
    'ABRUZZO', 'BASILICATA', 'CALABRIA', 'CAMPANIA', 'EMILIA-ROMAGNA',
    'FRIULI VENEZIA GIULIA', 'LAZIO', 'LIGURIA', 'LOMBARDIA', 'MARCHE',
    'MOLISE', 'PIEMONTE', 'PUGLIA', 'SARDEGNA', 'SICILIA', 'TOSCANA',
    'TRENTINO ALTO ADIGE', 'UMBRIA', 'VALLE D\'AOSTA', 'VENETO'
])

# Unisci i conteggi e le regioni italiane in un nuovo DataFrame
uominimapconto_df = pd.DataFrame({'regione': regioni_italiane.index, 'conta_uomini': regione_counts})
uominimapconto_df = uominimapconto_df.fillna(0).astype(int)

# Salva il nuovo DataFrame in un nuovo file CSV 'uominimapconto.csv'
uominimapconto_df.to_csv('uominimapconto.csv', index=False)
