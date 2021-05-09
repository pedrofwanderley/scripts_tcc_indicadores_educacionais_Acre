import pandas as pd
import pytz
from progress.spinner import Spinner

pd.options.mode.chained_assignment = None

REDE_ENSINO = {
    "Municipal": 1,
    "Estadual": 2,
    "Federal": 3,
    "Privada": 4,
    "Pública": 5
}

LOCALIDADE_ENSINO = {
    "Rural": 1,
    "Urbana": 2
}

MUNICIPIOS = {
    'Acrelândia': 3,
    'Assis Brasil': 4,
    'Brasiléia': 5,
    'Bujari': 6,
    'Capixaba': 7,
    'Cruzeiro do Sul':8,
    'Epitaciolândia':9,
    'Feijó':10,
    'Jordão': 11,
    'Mâncio Lima': 12,
    'Manoel Urbano': 13,
    'Marechal Thaumaturgo': 14,
    'Plácido de Castro': 15,
    'Porto Acre': 16,
    'Porto Walter': 17,
    'Rio Branco': 18,
    'Rodrigues Alves': 19,
    'Santa Rosa do Purus': 20,
    'Sena Madureira': 21,
    'Senador Guiomard': 22,
    'Tarauacá': 23,
    'Xapuri': 24
}

MUNICIPIOS_COD_IBGE = {
  1200013: 3,
  1200054: 4,
  1200104: 5,
  1200138: 6,
  1200179: 7,
  1200203: 8,
  1200252: 9,
  1200302: 10,
  1200328: 11,
  1200336: 12,
  1200344: 13,
  1200351: 14,
  1200385: 15,
  1200807: 16,
  1200393: 17,
  1200401: 18,
  1200427: 19,
  1200435: 20,
  1200500: 21,
  1200450: 22,
  1200609: 23,
  1200708: 24
}

spinner = Spinner('Lendo planilhas...')

def spin(x):
    spinner.next()
    return True

#Definir o arquivo que será lido
df = pd.read_csv('AFD_MUNICIPIOS_2019.csv', sep=";", low_memory=False,  usecols=spin)

# Definir tabela a qual serão gerados os inserts
current_table = 'AFD_MUNICIPIOS_2019'

# Definir as colunas do documento lido que serão associadas a cada coluna da tabela do banco
doc_columns = ['Ano','Sigla','Código do Município','Nome do Município'
 ,'Localização','Dependência Administrativa','Grupo 1 EI','Grupo 2 EI'
 ,'Grupo 3 EI','Grupo 4 EI','Grupo 5 EI']


# Definir os nomes das colunas da tabela do banco de dados (definir também os ids das chaves estrangeiras)
db_columns = ['ANO', 'G1_E1', 'G1_2','G1_3','G1_4','G1_5', 'MUNICIPIO_ID', 'REDE_ID', 'LOCALIDADE_ID']

# Definir as colunas que não serão utilizadas no insert mas são necessárias para o processamento 
unused_cols = ['Sigla','Código do Município','Nome do Município','Localização','Dependência Administrativa']


# Definir a coluna do documento que representa o estado da federação
# Filtrar pelo estado do Acre
df_acre = df[(df['Sigla'] == 'AC') & (df['Dependência Administrativa'] != 'Total') & (df['Localização'] != 'Total')]

insert_table = 'INSERT INTO ' + current_table

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
    fks = []
    for d in doc_columns:
        rede_id = None
        localidade_id = None
        municipio_id = None
        #Modificar linha caso insert precise de um Foreign Key de Município 
        if (d == 'Código do Município') :
            municipio_id = MUNICIPIOS_COD_IBGE[row[d]]
            fks.append(municipio_id)
        #Modificar linha caso insert precise de um Foreign Key de Rede de Ensino
        if (d == 'Dependência Administrativa') :
            rede_id = REDE_ENSINO[row[d]]
            fks.append(rede_id)
        #Modificar linha caso insert precise de um Foreign Key de Localidade de Ensino
        if (d == 'Localização') :
           localidade_id = LOCALIDADE_ENSINO[row[d]]
           fks.append(localidade_id)
        
        if(d not in unused_cols):
            if doc_columns.index(d) != len(doc_columns) -1:

                if(str(row[d]) == 'NULL'):
                    insert_values += str(row[d]) + ", "
                else:
                    insert_values += f"\'{str(row[d]).replace(',','.')}\'" + ", "
            else:
                if(str(row[d]) == 'NULL'):
                    insert_values += str(row[d])
                else:
                    insert_values += f"\'{str(row[d]).replace(',','.')}\'"
    for fk in fks:
        insert_values += ", " + str(fk)

    if (count < df_size):
        insert_values += '), ' + '\n'
    else:
        insert_values += ')'
    count += 1
 
output = insert_table + into_columns + insert_values

text_file = open("INSERTS_" + current_table + ".sql", "w")
text_file.write(output)
text_file.close()

spinner.finish()


