import sqlalchemy
import pandas as pd
import os

SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')


# Conectar ao banco de dados SQLite
ENGINE = sqlalchemy.create_engine(
    'sqlite:///' + os.path.join(PROCESSED_DIR, 'database.db')
)

# Listando arquivos no diretório RAW
arquivos_csv = [
    file_name for file_name in os.listdir(RAW_DIR) if file_name.endswith('.csv')
]

# Criando tabelas no banco de dados
for file in arquivos_csv:
    # Criando nome da tabela
    name_table = file.split('.')[0].replace('_dataset', '').replace('olist_', '')

    # Lendo arquivo csv como dataframe e inserindo no banco de dados
    df = pd.read_csv(os.path.join(RAW_DIR, file))
    df.to_sql(name_table, ENGINE, if_exists='replace', index=False)


            

    
    







# Criar uma tabela
#cursor.execute('''CREATE TABLE minha_tabela (coluna1, coluna2, coluna3)''')
#
## Ler dados do CSV e inserir na tabela
#with open('meu_arquivo.csv', 'r') as csvfile:
#    csvreader = csv.reader(csvfile)
#    for row in csvreader:
#        cursor.execute("INSERT INTO minha_tabela VALUES (?, ?, ?)", row)
#
## Salvar e fechar
#conn.commit()
#conn.close()