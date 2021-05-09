# scripts_tcc_indicadores_educacionais_Acre
Neste repositório estão disponíveis os scripts utilizados no processo de ETL para construção da ferramenta de BI que analisa indicadores educacionais no estado do Acre

## Como executar os scripts

### Passo 1:

- Selecione o arquivo que deseja gerar os inserts e traga-o para a pasta de scripts.
- Abra o arquivo em um editro de planilhas e exclua todas as linhas de cabeçalho, deixando apenas a linha que define as colunas.
- Salve o arquivo como um arquivo CSV

### Passo 2:

- No arquivo columns_scripts.py edite-o e adicione o nome do CSV salvo
- Execute o script e ele irá gerar um arquivo chamado `COLUMNS_` + <NOME ARQUIVO INSERIDO>
- Com esse arquivo você conseguirá pegar apenas as colunas que importam para o insert


### Passo 3:

- Agora no arquivo `script.py`, altere o arquivo que será lido para o CSV desejado
- Altere a variável `current_table` e coloque o nome da tabela que serão gerados os inserts
- Pegue as colunas selecionadas no arquvivo `COLUMNS_` + <NOME ARQUIVO INSERIDO> e adicione no array `doc_columns` (apenas as que serão utilizadas no insert)
- Altere o array `unused_columns` e adicione as colunas do documento que não devem estar no insert, mas são utilizadas para o processamento (codigo do municipio, rede, localidade, ...)
- Altere o array `db_columns` e insira as colunas da tabela a ser manipulada (Importante: Neste ponto a ordem das colunas deve ser as mesmas das colunas definidas no array `doc_columns`) (Lembre de colocar as FK no final do array caso necessário)
- Altere of filtros do `df_acre`, deve-se utilizar a coluna do csv que indique o estado vinculado ao dado. ex: `df_acre = df[(df['Sigla'] == 'AC') & (df['Dependência Administrativa'] != 'Total') & (df['Localização'] != 'Total')]`
