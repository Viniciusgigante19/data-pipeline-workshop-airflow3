from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os


### TASK 1 ###

def extract_data(**context):
    """Extrai dados do arquivo CSV"""

    file_path = '/opt/airflow/data/produtos_loja.csv'

    # Verifica se o arquivo existe
    if not os.path.exists(file_path):
        logging.error("Arquivo não encontrado!")
        return  # ou raise Exception("Arquivo não encontrado!")

    logging.info(f"Extraindo dados de: {file_path}")

    # Lê o CSV
    df = pd.read_csv(file_path)

    # Log do número de registros extraídos
    logging.info(f"Número de registros extraídos: {len(df)}")
    logging.info(f"Número de colunas: {df.shape[1]}")


### TASK 2 ###
def extract_vendas(**context):
    file_path = '/opt/airflow/data/vendas_produtos.csv'

     # Verifica se o arquivo existe
    if not os.path.exists(file_path):
        logging.error("Arquivo não encontrado!")
        return  # ou raise Exception("Arquivo não encontrado!")

    logging.info(f"Extraindo dados de: {file_path}")

    # Lê o CSV
    df = pd.read_csv(file_path)

    # Log do número de registros extraídos
    logging.info(f"Número de registros extraídos: {len(df)}")
    logging.info(f"Número de colunas: {df.shape[1]}")


### TASK 3 ###
def transform_data(**context):
    """Transforma os dados extraídos"""
    logging.info("Iniciando transformação dos dados")

    df = pd.read_csv('/tmp/dados_extraidos.csv')

    