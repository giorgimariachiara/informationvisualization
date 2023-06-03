import pandas as pd

# Carica il file CSV
df = pd.read_csv('totaledeputatiperlegislatura.csv')

# Calcola il conteggio di uomini e donne per ogni legislatura
conteggio_sesso_legislatura = df.groupby(['legislatura', 'gender']).size().unstack(fill_value=0)

# Aggiungi una riga di totale per ogni legislatura
conteggio_sesso_legislatura['Totale'] = conteggio_sesso_legislatura.sum(axis=1)

# Resetta l'indice per avere 'legislatura' come colonna
conteggio_sesso_legislatura = conteggio_sesso_legislatura.reset_index()

# Rinomina le colonne
conteggio_sesso_legislatura.columns = ['legislatura', 'female', 'male', 'Totale']

# Salva il nuovo DataFrame in un nuovo file CSV
conteggio_sesso_legislatura.to_csv('contoxlegi.csv', index=False)
