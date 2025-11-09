# Testando o Data Pipeline (Airflow)

Este guia mostra como testar o pipeline de produtos e vendas sem abordar build ou configuração do Docker. Ele cobre como entrar no container, verificar DAGs, disparar execuções, acompanhar logs e monitorar a saúde dos containers.

### Pré-requisitos
- Container do Airflow e Postgres rodando via Docker Compose
- DAG `pipeline_produtos_vendas` presente na pasta `dags/`

### Comandos para testar o pipeline
```bash
# Entrar no container do Airflow
docker exec -it data-pipeline-workshop-airflow3-airflow-standalone-1 bash

# Verificar DAGs carregadas
airflow dags list

# Verificar Logs das Dags 
airflow dags logs pipeline_produtos_vendas

# Disparar DAG manualmente
airflow dags trigger pipeline_produtos_vendas

# Despausar a DAG
airflow dags unpause pipeline_produtos_vendas

# Listar Tasks
airflow tasks list pipeline_produtos_vendas

extract_produtos
extract_vendas
transform_data
create_tables
load_data
generate_report

# Acompanhar execuções
airflow dags list-runs -d pipeline_produtos_vendas
airflow tasks list pipeline_produtos_vendas

# Logs do container Airflow
docker logs -f data-pipeline-workshop-airflow3-airflow-standalone-1

# Logs do Postgres (opcional)
docker logs -f --tail 200 postgres-erp
