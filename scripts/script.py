import pandas as pd
import pytz
from progress.spinner import Spinner

# xls = pd.ExcelFile('teste.xls')
# df1 = pd.read_excel(xls, header=[1], 
#                     sheet_name='TDI NORTE')


def getFkId(cursor, cod_mun, rede, localidade):
    result = []
    id_mun = "select ID from MUNICIPIO where CODIGO=" + str(cod_mun)
    cursor.execute(id_mun)
    record_mun = cursor.fetchone()
    if (record_mun and record_mun[0]): result.append(record_mun[0])
    id_rede = "select ID from REDE_ENSINO where SUB_REDE='" + rede + "'"
    cursor.execute(id_rede)
    record_rede = cursor.fetchone()
    if (record_rede and record_rede[0]): result.append(record_rede[0])
    id_loc = "select ID from LOCALIDADE_ENSINO where LOCALIDADE='" + localidade + "'"
    cursor.execute(id_loc)
    record_loc = cursor.fetchone()
    if (record_loc and record_loc[0]): result.append(record_loc[0])

    return result

def getFkMun(cursor, cod_mun):
    result = []
    id_mun = "select ID from MUNICIPIO where CODIGO=" + str(cod_mun)
    cursor.execute(id_mun)
    record_mun = cursor.fetchone()
    if (record_mun and record_mun[0]): result.append(record_mun[0])

    return result

def getFkMunNomeUf(cursor, nome, uf):
    result = []
    id_mun = "select ID from MUNICIPIO where UF= '" + uf + "' AND NOME = '"+nome+"' COLLATE SQL_Latin1_General_CP1_CI_AI"
    cursor.execute(id_mun)
    record_mun = cursor.fetchone()
    if (record_mun and record_mun[0]): result.append(record_mun[0])

    return result


spinner = Spinner('Lendo planilhas...')

def spin(x):
    spinner.next()
    return True


xls = pd.ExcelFile('AFD_MUNICIPIOS_2015.ods')
df = pd.read_excel(xls, header=[0], 
                    sheet_name='Ind__adeq__form__doc_', usecols=spin)


# doc_columns = ['Ano', 'UF', 'Município', 'Código do Município', 'Localização', 'Rede', '1ª a 4ª Série/1º ao 5º Ano'
#  ,'5ª a 8ª Série/6º ao 9º Ano', 'Total Fundamental', ' 1ª Série', ' 2ª Série',
#  ' 3ª Série', ' 4ª Série', 'Total Médio']

doc_columns = ['Ano', 'Sigla', 'Código do Município', 'Nome do Município',
 'Localização', 'Dependência Administrativa', 'Grupo 1 EI', 'Grupo 2 EI',
 'Grupo 3 EI', 'Grupo 4 EI', 'Grupo 5 EI']


current_table = 'AFD_MUNICIPIOS_2015'

db_columns = ['ANO', 'UF', 'COD_MUN', 'COD_MUNICIPIO','MUNICIPIO', 'LOCALIDADE', 'REDE', 'G1_E1', 'G1_2','G1_3','G1_4','G1_5']


# df_acre = df1[df1['UF'] == 'AC']
df_acre = df[df['Sigla'] == 'AC']

insert_string = 'INSERT INTO ' + current_table

for col in df_acre.columns.values:
    df_acre[col] = df_acre[col].replace(['--'], 'NULL')

into_columns = ' ('
for col in db_columns:
    if db_columns.index(col) != len(db_columns) -1:
        into_columns += col + ", "
    else:
        into_columns += col + ") VALUES \n"

df_size = len(df_acre)

insert_values = ''
count = 1
for index, row in df_acre.iterrows():
    
    insert_values += '('

    for d in doc_columns:
        if doc_columns.index(d) != len(doc_columns) -1:
            if(str(row[d]) == 'NULL'):
                insert_values += str(row[d]) + ", "
            else:
                insert_values += f"\'{str(row[d])}\'" + ", "
        else:
            if(str(row[d]) == 'NULL'):
                insert_values += str(row[d]) + ", "
            else:
                insert_values += f"\'{str(row[d])}\'"

    if (count < df_size):
        insert_values += '), ' + '\n'
    else:
        insert_values += ')'
    count += 1
 
output = insert_string + into_columns + insert_values

text_file = open("INSERTS_" + current_table + ".sql", "w")
text_file.write(output)
text_file.close()


