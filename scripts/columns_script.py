import pandas as pd
from progress.spinner import Spinner

spinner = Spinner('Lendo planilhas...')

def spin(x):
    spinner.next()
    return True

def listToString(input_list):
    result = ''
    for e in input_list:
        result += e + ', '

    return result

# Altere aqui para o nome do CSV para ser executado
current_file = 'AFD_MUNICIPIOS_2019'

df = pd.read_csv(current_file + '.csv', nrows=2, sep=";", usecols=spin)


columns = listToString(df.columns.values.tolist())

text_file = open("COLUMNS_" + current_file + ".txt", "w")
text_file.write(columns[:len(columns)-2])
text_file.close()

spinner.finish()