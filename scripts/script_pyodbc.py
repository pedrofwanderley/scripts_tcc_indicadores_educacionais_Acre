
import pandas as pd 
import pyodbc
from datetime import datetime




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

def getPopulacaoNomeUf(cursor, nome, ano):
    result = []
    id_mun = "select POPULACAO_ESTIMADA from POPULACAO where ANO= " + str(ano) + " AND MUNICIPIO = '"+nome+"' COLLATE SQL_Latin1_General_CP1_CI_AI"
    cursor.execute(id_mun)
    record_mun = cursor.fetchone()
    if (record_mun and record_mun[0]): result.append(record_mun[0])
    return result

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

print ("digite qual tabela voce quer povoar: ")
entrada = raw_input()
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=DESKTOP-BO1SAJN;'
                        'Database=INDICADORES_EDUCACIONAIS_AC;'
                        'Trusted_Connection=yes;')
cursor = conn.cursor()

if entrada == "DISTORCAO IDADE SERIE":
    data = pd.read_csv (r'C:\Users\pwand\scripts\TDI_AC.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','REGIAO','UF','COD_MUN', 'NOME','LOC','REDE','TOTAL_EF','AI','AF',
    'ANO1','ANO2','ANO3','ANO4','ANO5','ANO6','ANO7','ANO8','ANO9','TOTAL_EM','EM1','EM2','EM3','EM4'])
    for row in df.itertuples():

        result = getFkId(cursor, row.COD_MUN, row.REDE, row.LOC.upper())

        if (len(result) == 3):
            mun_id = result[0]
            rede_id = result[1]
            loc_id = result[2]
            
            cursor.execute("INSERT INTO DISTORCAO_IDADE_SERIE (ANO, TX_ENSINO_FUNDAMENTAL, TX_ANOS_INICIAIS, TX_ANOS_FINAIS, TX_1_ANO_EF,TX_2_ANO_EF,TX_3_ANO_EF," +
            " TX_4_ANO_EF,TX_5_ANO_EF,TX_6_ANO_EF,TX_7_ANO_EF,TX_8_ANO_EF,TX_9_ANO_EF," +
            " TX_ENSINO_MEDIO, TX_1_SERIE_EM, TX_2_SERIE_EM, TX_3_SERIE_EM, TX_4_SERIE_EM," +
            " REDE_ID, LOCALIDADE_ID, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.ANO, row.TOTAL_EF, row.AI, row.AF, row.ANO1, row.ANO2, row.ANO3, row.ANO4,row.ANO5,row.ANO6,row.ANO7,row.ANO8,
                row.ANO9, row.TOTAL_EM, row.EM1, row.EM2, row.EM3, row.EM4, rede_id, loc_id, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")

elif entrada == "municipio":
    data = pd.read_csv (r'C:\Users\pwand\scripts\municipios_ac.csv', encoding='UTF-8', delimiter=';')   
    df = pd.DataFrame(data, columns= ['UF',	'NOME_MESO',	'NOME_MICRO',	'COD_MUN',	'NOME_MUN'	,'latitude','longitude'])
    for row in df.itertuples():
        cursor.execute("INSERT INTO MUNICIPIO (UF, MESORREGIAO, MICRORREGIAO, CODIGO, NOME, LATITUDE, LONGITUDE) VALUES (?,?,?,?,?,?,?)",
            (row.UF,row.NOME_MESO, row.NOME_MICRO, row.COD_MUN, row.NOME_MUN, row.latitude, row.longitude))              
    conn.commit()
    print ("Dados inseridos com sucesso!")


elif entrada == "rede":
    data = pd.read_csv (r'C:\Users\pwand\scripts\rede.csv', encoding='UTF-8', delimiter=';')   
    df = pd.DataFrame(data, columns= ['REDE',	'SUB_REDE'])
    for row in df.itertuples():
        cursor.execute("INSERT INTO REDE_ENSINO (REDE, SUB_REDE) VALUES (?,?)",
            (row.REDE, row.SUB_REDE))              
    conn.commit()
    print ("Dados inseridos com sucesso!")

elif entrada == "localidade":
    data = pd.read_csv (r'C:\Users\pwand\scripts\localidade.csv', encoding='UTF-8')   
    df = pd.DataFrame(data, columns= ['LOCALIDADE'])
    for row in df.itertuples():
        cursor.execute("INSERT INTO LOCALIDADE_ENSINO (LOCALIDADE) VALUES (?)",
            (row.LOCALIDADE))              
    conn.commit()
    print ("Dados inseridos com sucesso!")

elif entrada == "ana":
    data = pd.read_csv (r'C:\Users\pwand\scripts\ANA.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['Ano','UF','COD_MUN','NOME','REDE','LOC','L1','L2','L3','L4','E1','E2','E3','E4','E5','M1','M2','M3','M4'])
    for row in df.itertuples():
        id_mun = "select ID from MUNICIPIO where CODIGO=" + str(row.COD_MUN)
        cursor.execute(id_mun)
        record_mun = cursor.fetchone()
        id_rede = "select ID from REDE_ENSINO where SUB_REDE='" + row.REDE[0] + "'"
        cursor.execute(id_rede)
        record_rede = cursor.fetchone()
        id_loc = "select ID from LOCALIDADE_ENSINO where LOCALIDADE='" + row.LOC.upper() + "' COLLATE SQL_Latin1_General_CP1_CI_AI"
        cursor.execute(id_loc)
        record_loc = cursor.fetchone()


        if record_mun and record_mun[0] and record_rede and record_rede[0] and record_loc and record_loc[0]:
            mun_id = record_mun[0]
            rede_id = record_rede[0]
            loc_id = record_loc[0]
            
            cursor.execute("INSERT INTO AVALIACAO_NAC_ALFABETIZACAO (ANO, LEITURA_NVL_1,LEITURA_NVL_2, LEITURA_NVL_3, LEITURA_NVL_4,ESCRITA_NVL_1,ESCRITA_NVL_2, ESCRITA_NVL_3,ESCRITA_NVL_4,ESCRITA_NVL_5,MATEMATICA_NVL_1,MATEMATICA_NVL_2,MATEMATICA_NVL_3,MATEMATICA_NVL_4, REDE_ID, LOCALIDADE_ID, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.Ano, row.L1, row.L2, row.L3, row.L4, row.E1, row.E2, row.E3, row.E4, row.E5, row.M1, row.M2, row.M3, row.M4, rede_id, loc_id, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")


elif entrada == "prova brasil":
    data = pd.read_csv (r'C:\Users\pwand\scripts\prova_brasil.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','UF','COD_MUN','NOME','REDE','LOC','MED_AI_LPT','MED_AI_MAT','MED_AF_LPT','MED_AF_MAT'])
    for row in df.itertuples():
        result = getFkId(cursor, row.COD_MUN, row.REDE, row.LOC)

        
        if (len(result) == 3):
            mun_id = result[0]
            rede_id = result[1]
            loc_id = result[2]
            
            cursor.execute("INSERT INTO PROVA_BRASIL (ANO, MEDIA_ANOS_INICIAIS_LPT, MEDIA_ANOS_FINAIS_LPT, MEDIA_ANOS_INICIAIS_MAT, MEDIA_ANOS_FINAIS_MAT, REDE_ID, LOCALIDADE_ID, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?)",
                (row.ANO, row.MED_AI_LPT, row.MED_AF_LPT, row.MED_AI_MAT, row.MED_AF_MAT, rede_id, loc_id, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")




elif entrada == "AFD":
    data = pd.read_csv (r'C:\Users\pwand\scripts\AFD_AF.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','UF','COD_MUN','NOME','LOC','REDE','MOD','G1','G2','G3','G4','G5'])
    for row in df.itertuples():
        result = getFkId(cursor, row.COD_MUN, row.REDE, row.LOC)

        if (len(result) == 3):
            mun_id = result[0]
            rede_id = result[1]
            loc_id = result[2]
            
            cursor.execute("INSERT INTO ADEQUACAO_FORMACAO_DOCENTE (ANO, MODALIDADE, TX_GRUPO_1, TX_GRUPO_2, TX_GRUPO_3, TX_GRUPO_4, TX_GRUPO_5, REDE_ID, LOCALIDADE_ID, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (row.ANO, row.MOD,row.G1,row.G2,row.G3,row.G4,row.G5, rede_id, loc_id, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")

elif entrada == "IED":
    data = pd.read_csv (r'C:\Users\pwand\scripts\IED_EM.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','UF','COD_MUN','NOME','LOC','REDE','MOD','N1','N2','N3','N4','N5','N6'])
    for row in df.itertuples():
        result = getFkId(cursor, row.COD_MUN, row.REDE, row.LOC)

        if (len(result) == 3):
            mun_id = result[0]
            rede_id = result[1]
            loc_id = result[2]
            
            cursor.execute("INSERT INTO ESFORCO_DOCENTE (ANO, MODALIDADE, TX_NIVEL_1, TX_NIVEL_2, TX_NIVEL_3, TX_NIVEL_4, TX_NIVEL_5, TX_NIVEL_6, REDE_ID, LOCALIDADE_ID, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (row.ANO, row.MOD,row.N1,row.N2,row.N3,row.N4,row.N5,row.N6, rede_id, loc_id, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")


elif entrada == "EVASAO":
    data = pd.read_csv (r'C:\Users\pwand\scripts\EVASAO_AC.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','UF','NOME','COD_MUN','REDE','TOTAL_EF','AI','AF','ANO1','ANO2','ANO3','ANO4','ANO5','ANO6','ANO7','ANO8','ANO9','TOTAL_EM','EM1','EM2','EM3'])
    for row in df.itertuples():
        result = getFkMun(cursor, row.COD_MUN)

        if (len(result) == 1):
            mun_id = result[0]
            
            cursor.execute("INSERT INTO TRANSICAO_EVASAO (ANO, REDE, TX_EF, TX_ANOS_INICIAIS, TX_ANOS_FINAIS, TX_1_ANO_EF, TX_2_ANO_EF, TX_3_ANO_EF,TX_4_ANO_EF,TX_5_ANO_EF,TX_6_ANO_EF,TX_7_ANO_EF,TX_8_ANO_EF,TX_9_ANO_EF, TX_EM, TX_1_SERIE_EM,TX_2_SERIE_EM,TX_3_SERIE_EM, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.ANO, row.REDE,row.TOTAL_EF, row.AI, row.AF, row.ANO1, row.ANO2, row.ANO3, row.ANO4, row.ANO5, row.ANO6, row.ANO7, row.ANO8, row.ANO9, row.TOTAL_EM, row.EM1, row.EM2, row.EM3, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")


elif entrada == "REPETENCIA":
    data = pd.read_csv (r'C:\Users\pwand\scripts\REPETENCIA_AC.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','UF','NOME','COD_MUN','REDE','TOTAL_EF','AI','AF','ANO1','ANO2','ANO3','ANO4','ANO5','ANO6','ANO7','ANO8','ANO9','TOTAL_EM','EM1','EM2','EM3'])
    for row in df.itertuples():
        result = getFkMun(cursor, row.COD_MUN)

        if (len(result) == 1):
            mun_id = result[0]
            
            cursor.execute("INSERT INTO TRANSICAO_REPETENCIA (ANO, REDE, TX_EF, TX_ANOS_INICIAIS, TX_ANOS_FINAIS, TX_1_ANO_EF, TX_2_ANO_EF, TX_3_ANO_EF,TX_4_ANO_EF,TX_5_ANO_EF,TX_6_ANO_EF,TX_7_ANO_EF,TX_8_ANO_EF,TX_9_ANO_EF, TX_EM, TX_1_SERIE_EM,TX_2_SERIE_EM,TX_3_SERIE_EM, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.ANO, row.REDE,row.TOTAL_EF, row.AI, row.AF, row.ANO1, row.ANO2, row.ANO3, row.ANO4, row.ANO5, row.ANO6, row.ANO7, row.ANO8, row.ANO9, row.TOTAL_EM, row.EM1, row.EM2, row.EM3, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")

elif entrada == "MEDIA ALUNOS":
    data = pd.read_csv (r'C:\Users\pwand\scripts\MAP_AC.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','REGIAO','UF','COD_MUN','NOME','LOC','REDE','TOTAL_EI','CRECHE','PRE_ESCOLA','TOTAL_EF','AI','AF','ANO1','ANO2','ANO3','ANO4','ANO5','ANO6','ANO7','ANO8','ANO9','UNIFICADAS','TOTAL_EM','EM1','EM2','EM3','EM4'])
    for row in df.itertuples():
        result = getFkId(cursor, row.COD_MUN, row.REDE, row.LOC)

        if (len(result) == 3):
            mun_id = result[0]
            rede_id = result[1]
            loc_id = result[2]
            
            cursor.execute("INSERT INTO MEDIA_ALUNOS (ANO, MED_EDUCACAO_INFANTIL, MED_CRECHE, MED_PRE_ESCOLA, MED_ENSINO_FUNDAMENTAL, MED_ANOS_INICIAIS, MED_ANOS_FINAIS, MED_1_ANO_EF, MED_2_ANO_EF, MED_3_ANO_EF, MED_4_ANO_EF, MED_5_ANO_EF, MED_6_ANO_EF, MED_7_ANO_EF, MED_8_ANO_EF, MED_9_ANO_EF, MED_ENSINO_MEDIO, MED_1_SERIE_EM, MED_2_SERIE_EM, MED_3_SERIE_EM, MED_4_SERIE_EM, REDE_ID, LOCALIDADE_ID, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.ANO, row.TOTAL_EI, row.CRECHE, row.PRE_ESCOLA, row.TOTAL_EF, row.AI, row.AF, row.ANO1, row.ANO2, row.ANO3, row.ANO4,row.ANO5,row.ANO6,row.ANO7,row.ANO8,row.ANO9, row.TOTAL_EM, row.EM1, row.EM2, row.EM3, row.EM4, rede_id, loc_id, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")




elif entrada == "ENSINO INTEGRAL":
    data = pd.read_csv (r'C:\Users\pwand\scripts\TIPO_ENSINO_AC.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','UF','NOME','ETAPA_ENSINO','TOTAL','TOTAL_PARCIAL','FED_PARCIAL','EST_PARCIAL','MUN_PARCIAL','PRIV_PARCIAL','TOTAL_INTEGRAL','FED_INTEGRAL','EST_INTEGRAL','MUN_INTEGRAL','PRIV_INTEGRAL'])
    for row in df.itertuples():

        result = getFkMunNomeUf(cursor, row.NOME.upper(), row.UF)

        if (len(result) == 1):
            mun_id = result[0]

            
            cursor.execute("INSERT INTO TIPO_ENSINO (ANO,TOTAL_MATRICULAS,TOTAL_PARCIAL,TOTAL_INTEGRAL,PARCIAL_FEDERAL,PARCIAL_ESTADUAL,PARCIAL_MUNICIPAL,PARCIAL_PRIVADA,INTEGRAL_FEDERAL,INTEGRAL_ESTADUAL,INTEGRAL_MUNICIPAL,INTEGRAL_PRIVADA,ETAPA_ENSINO,MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.ANO, row.TOTAL, row.TOTAL_PARCIAL, row.TOTAL_INTEGRAL, row.FED_PARCIAL,row.EST_PARCIAL,row.MUN_PARCIAL,row.PRIV_PARCIAL,row.FED_INTEGRAL,row.EST_INTEGRAL,row.MUN_INTEGRAL, row.PRIV_INTEGRAL, row.ETAPA_ENSINO, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")

elif entrada == "ATENDIMENTO EI":
    data = pd.read_csv (r'C:\Users\pwand\scripts\ATENDIMENTO_EI.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['ANO','UF','ETAPA_ENSINO','NOME','TOTAL','FEDERAL','ESTADUAL','MUNICIPAL','PRIVADA'])
    for row in df.itertuples():

        result = getFkMunNomeUf(cursor, row.NOME.upper(), row.UF)

        if (len(result) == 1):
            mun_id = result[0]

            
            cursor.execute("INSERT INTO ATENDIMENTO_EDUCACAO_INFANTIL (ANO,ETAPA_ENSINO,TOTAL_MATRICULAS,MATRICULAS_FEDERAL,MATRICULAS_ESTADUAL,MATRICULAS_MUNICIPAL,MATRICULAS_PRIVADA,MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?)",
                (row.ANO, row.ETAPA_ENSINO,row.TOTAL,row.FEDERAL, row.ESTADUAL, row.MUNICIPAL, row.PRIVADA, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")


elif entrada == "ESCOLA CENSO":
    data = pd.read_csv (r'C:\Users\pwand\scripts\ESCOLA_CENSO_2018.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['NU_ANO_CENSO','CO_ENTIDADE','NO_ENTIDADE','CO_ORGAO_REGIONAL','TP_SITUACAO_FUNCIONAMENTO','DT_ANO_LETIVO_INICIO','DT_ANO_LETIVO_TERMINO','CO_REGIAO','CO_MESORREGIAO','CO_MICRORREGIAO','CO_UF','CO_MUNICIPIO','CO_DISTRITO','TP_DEPENDENCIA','TP_LOCALIZACAO','TP_CATEGORIA_ESCOLA_PRIVADA','IN_CONVENIADA_PP','TP_CONVENIO_PODER_PUBLICO','IN_MANT_ESCOLA_PRIVADA_EMP','IN_MANT_ESCOLA_PRIVADA_ONG','IN_MANT_ESCOLA_PRIVADA_SIND','IN_MANT_ESCOLA_PRIVADA_SIST_S','IN_MANT_ESCOLA_PRIVADA_S_FINS','CO_ESCOLA_SEDE_VINCULADA','CO_IES_OFERTANTE','TP_REGULAMENTACAO','IN_LOCAL_FUNC_PREDIO_ESCOLAR','TP_OCUPACAO_PREDIO_ESCOLAR','IN_LOCAL_FUNC_SALAS_EMPRESA','IN_LOCAL_FUNC_SOCIOEDUCATIVO','IN_LOCAL_FUNC_UNID_PRISIONAL','IN_LOCAL_FUNC_PRISIONAL_SOCIO','IN_LOCAL_FUNC_TEMPLO_IGREJA','IN_LOCAL_FUNC_CASA_PROFESSOR','IN_LOCAL_FUNC_GALPAO','TP_OCUPACAO_GALPAO','IN_LOCAL_FUNC_SALAS_OUTRA_ESC','IN_LOCAL_FUNC_OUTROS','IN_PREDIO_COMPARTILHADO','IN_AGUA_FILTRADA','IN_AGUA_REDE_PUBLICA','IN_AGUA_POCO_ARTESIANO','IN_AGUA_CACIMBA','IN_AGUA_FONTE_RIO','IN_AGUA_INEXISTENTE','IN_ENERGIA_REDE_PUBLICA','IN_ENERGIA_GERADOR','IN_ENERGIA_OUTROS','IN_ENERGIA_INEXISTENTE','IN_ESGOTO_REDE_PUBLICA','IN_ESGOTO_FOSSA','IN_ESGOTO_INEXISTENTE','IN_LIXO_COLETA_PERIODICA','IN_LIXO_QUEIMA','IN_LIXO_JOGA_OUTRA_AREA','IN_LIXO_RECICLA','IN_LIXO_ENTERRA','IN_LIXO_OUTROS','IN_SALA_DIRETORIA','IN_SALA_PROFESSOR','IN_LABORATORIO_INFORMATICA','IN_LABORATORIO_CIENCIAS','IN_SALA_ATENDIMENTO_ESPECIAL','IN_QUADRA_ESPORTES_COBERTA','IN_QUADRA_ESPORTES_DESCOBERTA','IN_QUADRA_ESPORTES','IN_COZINHA','IN_BIBLIOTECA','IN_SALA_LEITURA','IN_BIBLIOTECA_SALA_LEITURA','IN_PARQUE_INFANTIL','IN_BERCARIO','IN_BANHEIRO_FORA_PREDIO','IN_BANHEIRO_DENTRO_PREDIO','IN_BANHEIRO_EI','IN_BANHEIRO_PNE','IN_DEPENDENCIAS_PNE','IN_SECRETARIA','IN_BANHEIRO_CHUVEIRO','IN_REFEITORIO','IN_DESPENSA','IN_ALMOXARIFADO','IN_AUDITORIO','IN_PATIO_COBERTO','IN_PATIO_DESCOBERTO','IN_ALOJAM_ALUNO','IN_ALOJAM_PROFESSOR','IN_AREA_VERDE','IN_LAVANDERIA','IN_DEPENDENCIAS_OUTRAS','QT_SALAS_EXISTENTES','QT_SALAS_UTILIZADAS','IN_EQUIP_TV','IN_EQUIP_VIDEOCASSETE','IN_EQUIP_DVD','IN_EQUIP_PARABOLICA','IN_EQUIP_COPIADORA','IN_EQUIP_RETROPROJETOR','IN_EQUIP_IMPRESSORA','IN_EQUIP_IMPRESSORA_MULT','IN_EQUIP_SOM','IN_EQUIP_MULTIMIDIA','IN_EQUIP_FAX','IN_EQUIP_FOTO','IN_COMPUTADOR','QT_EQUIP_TV','QT_EQUIP_VIDEOCASSETE','QT_EQUIP_DVD','QT_EQUIP_PARABOLICA','QT_EQUIP_COPIADORA','QT_EQUIP_RETROPROJETOR','QT_EQUIP_IMPRESSORA','QT_EQUIP_IMPRESSORA_MULT','QT_EQUIP_SOM','QT_EQUIP_MULTIMIDIA','QT_EQUIP_FAX','QT_EQUIP_FOTO','QT_COMPUTADOR','QT_COMP_ADMINISTRATIVO','QT_COMP_ALUNO','IN_INTERNET','IN_BANDA_LARGA','QT_FUNCIONARIOS','IN_ALIMENTACAO','TP_AEE','TP_ATIVIDADE_COMPLEMENTAR','IN_FUNDAMENTAL_CICLOS','TP_LOCALIZACAO_DIFERENCIADA','IN_MATERIAL_ESP_QUILOMBOLA','IN_MATERIAL_ESP_INDIGENA','IN_MATERIAL_ESP_NAO_UTILIZA','IN_EDUCACAO_INDIGENA','TP_INDIGENA_LINGUA','CO_LINGUA_INDIGENA','IN_BRASIL_ALFABETIZADO','IN_FINAL_SEMANA','IN_FORMACAO_ALTERNANCIA','IN_MEDIACAO_PRESENCIAL','IN_MEDIACAO_SEMIPRESENCIAL','IN_MEDIACAO_EAD','IN_ESPECIAL_EXCLUSIVA','IN_REGULAR','IN_EJA','IN_PROFISSIONALIZANTE','IN_COMUM_CRECHE','IN_COMUM_PRE','IN_COMUM_FUND_AI','IN_COMUM_FUND_AF','IN_COMUM_MEDIO_MEDIO','IN_COMUM_MEDIO_INTEGRADO','IN_COMUM_MEDIO_NORMAL','IN_ESP_EXCLUSIVA_CRECHE','IN_ESP_EXCLUSIVA_PRE','IN_ESP_EXCLUSIVA_FUND_AI','IN_ESP_EXCLUSIVA_FUND_AF','IN_ESP_EXCLUSIVA_MEDIO_MEDIO','IN_ESP_EXCLUSIVA_MEDIO_INTEGR','IN_ESP_EXCLUSIVA_MEDIO_NORMAL','IN_COMUM_EJA_FUND','IN_COMUM_EJA_MEDIO','IN_COMUM_EJA_PROF','IN_ESP_EXCLUSIVA_EJA_FUND','IN_ESP_EXCLUSIVA_EJA_MEDIO','IN_ESP_EXCLUSIVA_EJA_PROF','IN_COMUM_PROF','IN_ESP_EXCLUSIVA_PROF'])
    for row in df.itertuples():

        result = getFkId(cursor, row.CO_MUNICIPIO, row.TP_DEPENDENCIA, row.TP_LOCALIZACAO.upper())

        if (len(result) == 3):
            mun_id = result[0]
            rede_id = result[1]
            loc_id = result[2]
            
            cursor.execute("INSERT INTO ESCOLA_CENSO (NU_ANO_CENSO,NO_ENTIDADE,IN_AGUA_FILTRADA,IN_AGUA_REDE_PUBLICA,IN_AGUA_INEXISTENTE,IN_ENERGIA_REDE_PUBLICA,IN_ENERGIA_INEXISTENTE,IN_ESGOTO_REDE_PUBLICA,IN_ESGOTO_INEXISTENTE,IN_LABORATORIO_INFORMATICA,IN_LABORATORIO_CIENCIAS,IN_SALA_ATENDIMENTO_ESPECIAL,IN_QUADRA_ESPORTES,IN_COZINHA,IN_BIBLIOTECA,IN_PARQUE_INFANTIL,IN_BERCARIO,IN_BANHEIRO_FORA_PREDIO,IN_BANHEIRO_DENTRO_PREDIO,IN_BANHEIRO_EI,IN_BANHEIRO_PNE,IN_DEPENDENCIAS_PNE,IN_REFEITORIO,IN_DESPENSA,IN_AUDITORIO,IN_PATIO_COBERTO,IN_PATIO_DESCOBERTO,IN_AREA_VERDE,IN_EQUIP_COPIADORA,IN_EQUIP_RETROPROJETOR,IN_EQUIP_IMPRESSORA,IN_EQUIP_MULTIMIDIA,IN_COMPUTADOR,NU_EQUIP_COPIADORA,NU_EQUIP_RETROPROJETOR,NU_EQUIP_IMPRESSORA,NU_EQUIP_MULTIMIDIA,NU_COMPUTADOR,NU_COMP_ALUNO,IN_INTERNET,IN_BANDA_LARGA,IN_ALIMENTACAO,TP_AEE,IN_MEDIACAO_PRESENCIAL,IN_MEDIACAO_SEMIPRESENCIAL,IN_MEDIACAO_EAD,IN_ESPECIAL_EXCLUSIVA, REDE_ID, LOCALIDADE_ID, MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.NU_ANO_CENSO,row.NO_ENTIDADE,row.IN_AGUA_FILTRADA,row.IN_AGUA_REDE_PUBLICA,row.IN_AGUA_INEXISTENTE,row.IN_ENERGIA_REDE_PUBLICA,row.IN_ENERGIA_INEXISTENTE,row.IN_ESGOTO_REDE_PUBLICA,row.IN_ESGOTO_INEXISTENTE,row.IN_LABORATORIO_INFORMATICA,row.IN_LABORATORIO_CIENCIAS,row.IN_SALA_ATENDIMENTO_ESPECIAL,row.IN_QUADRA_ESPORTES,row.IN_COZINHA,row.IN_BIBLIOTECA,row.IN_PARQUE_INFANTIL,row.IN_BERCARIO,row.IN_BANHEIRO_FORA_PREDIO,row.IN_BANHEIRO_DENTRO_PREDIO,row.IN_BANHEIRO_EI,row.IN_BANHEIRO_PNE,row.IN_DEPENDENCIAS_PNE,row.IN_REFEITORIO,row.IN_DESPENSA,row.IN_AUDITORIO,row.IN_PATIO_COBERTO,row.IN_PATIO_DESCOBERTO,row.IN_AREA_VERDE,row.IN_EQUIP_COPIADORA,row.IN_EQUIP_RETROPROJETOR,row.IN_EQUIP_IMPRESSORA,row.IN_EQUIP_MULTIMIDIA,row.IN_COMPUTADOR,row.QT_EQUIP_COPIADORA,row.QT_EQUIP_RETROPROJETOR,row.QT_EQUIP_IMPRESSORA,row.QT_EQUIP_MULTIMIDIA,row.QT_COMPUTADOR,row.QT_COMP_ALUNO,row.IN_INTERNET,row.IN_BANDA_LARGA,row.IN_ALIMENTACAO,row.TP_AEE,row.IN_MEDIACAO_PRESENCIAL,row.IN_MEDIACAO_SEMIPRESENCIAL,row.IN_MEDIACAO_EAD,row.IN_ESPECIAL_EXCLUSIVA,rede_id, loc_id, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")



elif entrada == "POPULACAO":
    data = pd.read_csv (r'C:\Users\pwand\scripts\POPULACAO.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['CODIGO_IBGE','MUNICIPIO','ANO','POPULACAO'])
    for row in df.itertuples():

        result = getFkMun(cursor, row.CODIGO_IBGE)

        if (len(result) == 1):
            mun_id = result[0]
           
            cursor.execute("INSERT INTO POPULACAO (ANO,MUNICIPIO,COD_IBGE,POPULACAO_ESTIMADA,MUNICIPIO_ID) VALUES (?,?,?,?,?)",
                (row.ANO, row.MUNICIPIO,row.CODIGO_IBGE,row.POPULACAO, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")

elif entrada == "CUSTO ALUNO":
    data = pd.read_csv (r'C:\Users\pwand\scripts\CUSTO_ALUNO_MUNICIPIOS.csv', encoding='UTF-8', delimiter=";")   
    df = pd.DataFrame(data, columns= ['UF','MUNICIPIO','ESTIMATIVA_RECEITA','TOTAL_MATRICULAS','ANO','CUSTO_ALUNO'])
    for row in df.itertuples():

        result = getFkMunNomeUf(cursor,  row.MUNICIPIO, row.UF)
        resultPop = getPopulacaoNomeUf(cursor, row.MUNICIPIO, row.ANO)

        if (len(result) == 1 and len(resultPop) == 1):
            mun_id = result[0]
            pop = resultPop[0]
        
            
           
            cursor.execute("INSERT INTO CUSTO_ALUNO (ANO,MUNICIPIO,POPULACAO_ESTIMADA,CUSTO_ALUNO,TOTAL_MATRICULAS, ESTIMATIVA_RECEITA,MUNICIPIO_ID) VALUES (?,?,?,?,?,?,?)",
                (row.ANO, row.MUNICIPIO,pop,row.CUSTO_ALUNO,row.TOTAL_MATRICULAS,row.ESTIMATIVA_RECEITA, mun_id))              
    conn.commit()
    print ("Dados inseridos com sucesso!")








