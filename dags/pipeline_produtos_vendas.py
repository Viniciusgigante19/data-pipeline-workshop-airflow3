from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os


### TASK 1 ###

def extract_produtos(**context):
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

    df.to_csv('/temp/dados_extraidos_produtos.csv',index=False)

### TASK 2 ###
def extract_vendas(**context):
    file_path = '/opt/airflow/data/vendas_produtos.csv'

     # Verifica se o arquivo existe
    if not os.path.exists(file_path):
        logging.error("Arquivo não encontrado!")
        return 

    logging.info(f"Extraindo dados de: {file_path}")

    # Lê o CSV
    df = pd.read_csv(file_path)

    # Log do número de registros extraídos
    logging.info(f"Número de registros extraídos: {len(df)}")
    logging.info(f"Número de colunas: {df.shape[1]}")

    # Salva os dados extraídos
    df.to_csv('/temp/dados_extraidos_vendas.csv',index=False)


### TASK 3 ###
def transform_data(**context):
    """Transforma os dados extraídos"""
    logging.info("Iniciando transformação dos dados")

    # Carrega os dados extraídos
    df = pd.read_csv('/temp/dados_extraidos_produtos.csv')

    # --- Preencher Fornecedor nulo ---
    df['Fornecedor'] = df['Fornecedor'].fillna('Não informado')
    df['Fornecedor'] = df['Fornecedor'].replace('', 'Não informado')

    # --- Preencher Preco_Custo nulo com média da categoria ---
    df['Preco_Custo'] = pd.to_numeric(df['Preco_Custo'], errors='coerce')
    media_categoria = df.groupby('Categoria')['Preco_Custo'].transform('mean')
    df['Preco_Custo'] = df['Preco_Custo'].fillna(media_categoria)

    # --- Preencher Preco_Venda nulo com Preco_Custo * 1.3 ---
    df['Preco_Venda'] = pd.to_numeric(df['Preco_Venda'], errors='coerce')
    df['Preco_Venda'] = df['Preco_Venda'].fillna(df['Preco_Custo'] * 1.3)

    # Salva os dados transformados
    df.to_csv('/temp/dados_transformados_produtos.csv', index=False)
    logging.info(f"Transformação concluída: {len(df)} registros processados")
