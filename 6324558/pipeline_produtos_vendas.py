from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

default_args = {
    'owner':'Vinicius',
    'depends_on':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    dag_id='pipeline_produtos_vendas',
    default_args=default_args,
    description='Pipeline ETL de produtos e vendas',
    schedule='0 6 * * *',  
    catchup=False,                   
    tags=['produtos', 'vendas', 'exercicio'],
)

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

    # --- Carregar e integrar dados de vendas ---
    logging.info("Carregando dados de vendas para enriquecer produtos")
    vendas_path = '/opt/airflow/data/vendas_produtos.csv'
    vendas_df = pd.read_csv(vendas_path)

    # Garantir tipos corretos
    vendas_df['Quantidade_Vendida'] = pd.to_numeric(vendas_df['Quantidade_Vendida'], errors='coerce').fillna(0)
    vendas_df['Preco_Venda'] = pd.to_numeric(vendas_df['Preco_Venda'], errors='coerce').fillna(0)

    # Calcular total de vendas por produto
    vendas_agg = vendas_df.groupby('ID_Produto').agg({
        'Quantidade_Vendida': 'sum',
        'Preco_Venda': 'mean'
    }).reset_index()

    vendas_agg.rename(columns={
        'Quantidade_Vendida': 'Total_Vendido',
        'Preco_Venda': 'Media_Preco_Venda'
    }, inplace=True)

    # Fazer merge com o DataFrame principal
    df = df.merge(vendas_agg, on='ID_Produto', how='left')

    # Preencher nulos com 0 após merge
    df['Total_Vendido'] = df['Total_Vendido'].fillna(0)
    df['Media_Preco_Venda'] = df['Media_Preco_Venda'].fillna(df['Preco_Venda'])

    # --- Calcular Receita Total ---
    df['Receita_Total'] = df['Total_Vendido'] * df['Media_Preco_Venda']

    # Salvar dados transformados
    df.to_csv('/temp/dados_transformados_produtos.csv', index=False)
    logging.info(f"Transformação concluída: {len(df)} registros processados")


### TASK 4 ###
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    -- Criação da tabela de produtos processados
    CREATE TABLE IF NOT EXISTS produtos_processados (
        ID_Produto VARCHAR(10),
        Nome VARCHAR(100),
        Categoria VARCHAR(50),
        Fornecedor VARCHAR(100),
        Preco_Custo DECIMAL(10,2),
        Preco_Venda DECIMAL(10,2),
        Total_Vendido INTEGER,
        Media_Preco_Venda DECIMAL(10,2),
        Receita_Total DECIMAL(12,2)
    );

    -- Criação da tabela de vendas processadas
    CREATE TABLE IF NOT EXISTS vendas_processadas (
        ID_Venda VARCHAR(10),
        ID_Produto VARCHAR(10),
        Quantidade_Vendida INTEGER,
        Preco_Venda DECIMAL(10,2),
        Data_Venda DATE,
        Canal_Venda VARCHAR(50)
    );

    -- Criação da tabela de relatório de vendas (join dos dados)
    CREATE TABLE IF NOT EXISTS relatorio_vendas AS
    SELECT 
        v.ID_Venda,
        v.ID_Produto,
        p.Nome,
        p.Categoria,
        v.Quantidade_Vendida,
        v.Preco_Venda,
        v.Data_Venda,
        v.Canal_Venda,
        p.Total_Vendido,
        p.Receita_Total
    FROM vendas_processadas v
    LEFT JOIN produtos_processados p ON v.ID_Produto = p.ID_Produto
    WHERE 1=0; -- evita inserir dados agora, apenas cria estrutura
    """,
    dag=dag,
)


### TASK 5 ###
def load_data(**context):
    """Carrega dados transformados no PostgreSQL"""
    logging.info("Iniciando carga dos dados no PostgreSQL")

    # --- Conexão com o PostgreSQL ---
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()

    # --- Carregar dados transformados de produtos ---
    produtos_path = '/temp/dados_transformados_produtos.csv'
    logging.info(f"Lendo dados transformados de: {produtos_path}")
    df_produtos = pd.read_csv(produtos_path)

    # --- Carregar dados de vendas ---
    vendas_path = '/opt/airflow/data/vendas_produtos.csv'
    logging.info(f"Lendo dados de vendas de: {vendas_path}")
    df_vendas = pd.read_csv(vendas_path)

    # --- Inserir produtos processados ---
    logging.info("Inserindo dados na tabela produtos_processados")
    df_produtos.to_sql('produtos_processados', engine, if_exists='replace', index=False, method='multi')

    # --- Inserir vendas processadas ---
    logging.info("Inserindo dados na tabela vendas_processadas")
    df_vendas.to_sql('vendas_processadas', engine, if_exists='replace', index=False, method='multi')

    # --- Criar relatorio_vendas (join entre as tabelas) ---
    logging.info("Gerando tabela relatorio_vendas")
    with engine.connect() as conn:
        conn.execute("""
            DELETE FROM relatorio_vendas;
            INSERT INTO relatorio_vendas
            SELECT 
                v.ID_Venda,
                v.ID_Produto,
                p.Nome,
                p.Categoria,
                v.Quantidade_Vendida,
                v.Preco_Venda,
                v.Data_Venda,
                v.Canal_Venda,
                p.Total_Vendido,
                p.Receita_Total
            FROM vendas_processadas v
            LEFT JOIN produtos_processados p
            ON v.ID_Produto = p.ID_Produto;
        """)
        conn.commit()

    # --- Validação final ---
    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM relatorio_vendas;")
        total_registros = result.scalar()
        logging.info(f"Validação concluída: {total_registros} registros inseridos em relatorio_vendas")

    return f"Carga concluída com sucesso: {total_registros} registros inseridos em relatorio_vendas"


### TASK 6 ###
def generate_report(**context):
    """Gera relatório consolidado de vendas"""
    logging.info("Iniciando geração do relatório de vendas")

    # --- Conectar ao PostgreSQL ---
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()

    # --- Ler dados das tabelas ---
    df_produtos = pd.read_sql("SELECT * FROM produtos_processados", engine)
    df_vendas = pd.read_sql("SELECT * FROM vendas_processadas", engine)
    df_relatorio = pd.read_sql("SELECT * FROM relatorio_vendas", engine)

    # --- Total de vendas por categoria ---
    total_por_categoria = (
        df_relatorio.groupby('Categoria')['Receita_Total']
        .sum()
        .reset_index()
        .rename(columns={'Receita_Total': 'Total_Vendas'})
    )

    # --- Produto mais vendido ---
    produto_mais_vendido = (
        df_relatorio.groupby(['ID_Produto', 'Nome'])['Quantidade_Vendida']
        .sum()
        .reset_index()
        .sort_values('Quantidade_Vendida', ascending=False)
        .head(1)
    )

    # --- Canal de venda com maior receita ---
    canal_maior_receita = (
        df_relatorio.groupby('Canal_Venda')['Receita_Total']
        .sum()
        .reset_index()
        .sort_values('Receita_Total', ascending=False)
        .head(1)
    )

    # --- Margem de lucro média por categoria ---
    df_produtos['Margem_Lucro'] = (
        (df_produtos['Preco_Venda'] - df_produtos['Preco_Custo']) / df_produtos['Preco_Custo']
    ) * 100
    margem_media_categoria = (
        df_produtos.groupby('Categoria')['Margem_Lucro']
        .mean()
        .reset_index()
        .rename(columns={'Margem_Lucro': 'Margem_Lucro_Media'})
    )

    # --- Consolidar relatório final ---
    logging.info("Consolidando relatório final")
    resumo = {
        "total_vendas_por_categoria": total_por_categoria.to_dict(orient='records'),
        "produto_mais_vendido": produto_mais_vendido.to_dict(orient='records'),
        "canal_maior_receita": canal_maior_receita.to_dict(orient='records'),
        "margem_media_por_categoria": margem_media_categoria.to_dict(orient='records'),
    }

    # --- Exportar relatório para CSV ---
    relatorio_path = '/temp/relatorio_final.csv'
    total_por_categoria.to_csv(relatorio_path, index=False)
    logging.info(f"Relatório gerado com sucesso e salvo em: {relatorio_path}")

    return resumo

# TASK 1 DEFINIDA
extract_produtos_task = PythonOperator(
    task_id = 'extract_produtos',
    python_callable=extract_produtos,
    dag=dag
)

# TASK 2 DEFINIDA
extract_vendas_task = PythonOperator(
    task_id = 'extract_vendas',
    python_callable=extract_vendas,
    dag=dag
)

# TASK 3 DEFINIDA
transform_data_task = PythonOperator(
    task_id = 'transform_data',
    python_callable=transform_data,
    dag=dag
)

# TASK 5 DEFINIDA
load_data_task = PythonOperator(
    task_id = 'load_data',
    python_callable=load_data,
    dag=dag
)

# TASK 6 DEFINIDA
generate_report_task = PythonOperator(
    task_id = 'generate_report',
    python_callable=generate_report,
    dag=dag
)

[extract_produtos_task, extract_vendas_task] >> transform_data_task
transform_data_task >> create_tables >> load_data_task >> generate_report_task
