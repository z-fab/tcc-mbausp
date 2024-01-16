import sqlalchemy
import pandas as pd
import os

import logging

# Definindo caminhos diretórios
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(SRC_DIR)
LOG_DIR = os.path.join(BASE_DIR, "log")
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

# Criando configuracao de logging
logger = logging.getLogger("olist-churn-log")
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)
c_format = logging.Formatter(
    "%(asctime)s :: (%(name)s | %(filename)s) %(levelname)s - %(message)s"
)
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)

f_handler = logging.FileHandler(f"{os.path.join(LOG_DIR, 'olist-churn.log')}")
f_handler.suffix = "%Y%m%d"
f_handler.setLevel(logging.INFO)
f_format = logging.Formatter(
    "%(asctime)s :: (%(name)s | %(filename)s) %(levelname)s - %(message)s"
)
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)


# Conectar ao banco de dados SQLite
ENGINE = sqlalchemy.create_engine(
    "sqlite:///" + os.path.join(PROCESSED_DIR, "database.db")
)

# Listando arquivos no diretório RAW
arquivos_csv = [
    file_name for file_name in os.listdir(RAW_DIR) if file_name.endswith(".csv")
]

# Criando tabelas no banco de dados
for file in arquivos_csv:
    # Criando nome da tabela
    name_table = file.split(".")[0].replace("_dataset", "").replace("olist_", "")
    logger.info(f"Criando tabela {name_table}")

    # Lendo arquivo csv como dataframe e inserindo no banco de dados
    df = pd.read_csv(os.path.join(RAW_DIR, file))
    df.to_sql(name_table, ENGINE, if_exists="replace", index=False)
    logger.info(f"Tabela {name_table} criada com sucesso. Total de {len(df)} registros")
