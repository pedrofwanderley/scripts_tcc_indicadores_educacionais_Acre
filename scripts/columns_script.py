import pandas as pd
from progress.spinner import Spinner

spinner = Spinner('Lendo planilhas...')

def spin(x):
    spinner.next()
    return True

df = pd.read_csv('AFD_MUNICIPIOS_2019.csv', nrows=2, sep=";", usecols=spin)

print (df.columns.values.tolist())

spinner.finish()