# 🇧🇷 Pipeline Econômico Brasileiro

**Airflow + PySpark + Delta Lake + dbt + DuckDB**  
Orquestração real de indicadores econômicos públicos com TaskGroups, ACID transactions e Time Travel

---

## Por que este projeto existe

> Airflow aparece em praticamente toda vaga de DE — mas a maioria dos portfólios mostra DAGs com dois nós. Este projeto demonstra os padrões que realmente aparecem em produção: TaskGroups paralelos, ShortCircuitOperator para falha graceful, dependências entre grupos e integração com toda a stack moderna.

---

## Arquitetura da DAG

```
[ibge]  fetch_pib + fetch_pop → validate → bronze ──┐
[bcb]   fetch_selic + ipca + cambio → validate → bronze ──┼──► silver (ACID MERGE)
[ipea]  fetch_desemp + gini → validate → bronze ──────────┘         │
                                                               [gold] bridge → dbt run → dbt test
                                                                          │
                                                                   notify_success
```

Os 3 TaskGroups rodam **em paralelo**. O Silver só começa quando **todos** os bronzes terminam.

---

## APIs consumidas (gratuitas, sem autenticação)

| Fonte | Dados | Fallback |
|---|---|---|
| IBGE SIDRA | PIB trimestral, população por UF | BCB série 4380 (PIB acumulado) |
| Banco Central SGS | SELIC, IPCA, câmbio USD/BRL | — |
| IPEA Data | Desemprego PNAD, renda per capita | BCB série 7326 (rendimento médio) |

> **Nota sobre resiliência:** o IBGE e o IPEA têm endpoints instáveis. O pipeline usa o Banco Central como fallback automático — se o IBGE retornar 500, o PIB vem da série BCB 4380. O BCB nunca retorna 500.

---

## Início rápido

### 1. Clonar e instalar

```bash
git clone https://github.com/seu-usuario/econ-pipeline
cd econ-pipeline
python -m venv venv

# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Instalar pacotes dbt (obrigatório antes do primeiro run)

```bash
cd dbt_project
dbt deps
cd ..
```

> **Por que é necessário:** o `dbt deps` baixa o pacote `dbt-labs/dbt_utils` declarado em `packages.yml`. Sem ele, o `dbt run` falha. Precisa rodar apenas uma vez — ou após deletar a pasta `dbt_packages/`.

### 3. Executar o pipeline

```bash
# Pipeline completo + dashboard
python run.py

# Só pipeline (sem dashboard)
python run.py --only-pipeline

# Só dashboard (pipeline já rodou)
python run.py --only-dashboard

# Pular Spark (Delta Lake já existe, reprocessar só dbt)
python run.py --skip-spark

# Fontes específicas
python run.py --fontes bcb_selic bcb_ipca bcb_cambio
```

### 4. Com Airflow (Docker)

```bash
cd docker
docker compose build --no-cache   # primeira vez ou após mudar o Dockerfile
docker compose up -d
```

Acesse `http://localhost:8080` com `admin` / `admin`.

> **Atenção:** no Docker, o `dbt deps` é executado automaticamente pela task `dbt_deps` na DAG. Não é necessário rodar manualmente.

---

## Resolução de problemas comuns

### `dbt run` falha com `KeyError: snapshot_helper.sql`

Cache corrompido do dbt no Windows. Solução:

```powershell
cd dbt_project
Remove-Item -Recurse -Force dbt_packages
Remove-Item -Recurse -Force target
Remove-Item -Force package-lock.yml
dbt deps
```

### `packages.yml` inválido

O arquivo deve ter exatamente este formato (sem campo `name:`):

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
```

### `HADOOP_HOME and hadoop.home.dir are unset` (Windows)

O `spark_session.py` baixa o `winutils.exe` automaticamente na primeira execução. Se falhar, verifique conexão com internet — ele baixa de `github.com/cdarlint/winutils`.

### `SerializationException: Failed to deserialize` (DuckDB)

O arquivo `econ.duckdb` foi criado por uma versão diferente do DuckDB. Delete e rode novamente:

```powershell
Remove-Item data\warehouse\econ.duckdb
Remove-Item data\warehouse\econ.duckdb.wal   # se existir
python run.py --skip-spark
```

### Colunas `None` no dashboard (PIB, renda)

O IBGE e o IPEA têm instabilidades. O pipeline usa fallbacks do BCB automaticamente, mas se o Silver já foi construído sem esses dados, force um reprocessamento completo:

```powershell
# Apaga o Silver e o DuckDB para reprocessar do zero
Remove-Item -Recurse -Force data\delta\silver
Remove-Item data\warehouse\econ.duckdb
python run.py --only-pipeline
```

---

## Estrutura do projeto

```
econ_pipeline/
├── run.py                         # Entry point único
├── requirements.txt
├── _winutils.py                   # Fix HADOOP_HOME no Windows (no-op Linux/Mac)
│
├── ingestion/
│   └── apis.py                    # Clientes IBGE, BCB, IPEA com fallbacks
│
├── spark_jobs/
│   ├── spark_session.py           # Fábrica de SparkSession (Docker + Windows)
│   ├── bronze.py                  # CSV → Delta Lake Bronze
│   ├── silver.py                  # Bronze → Silver (ACID MERGE + deduplicação)
│   └── bridge.py                  # Silver → Parquet → DuckDB
│
├── airflow/
│   └── dags/
│       └── econ_pipeline.py       # DAG com TaskGroups paralelos
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml               # Conexão DuckDB
│   ├── packages.yml               # dbt-utils 1.3.0
│   └── models/
│       ├── silver/
│       │   └── sources.yml        # Declara silver.indicadores como source
│       └── gold/
│           ├── schema.yml         # Testes de qualidade
│           ├── gld_painel_macro.sql
│           ├── gld_serie_mensal.sql
│           └── gld_correlacoes.sql
│
├── dashboard/
│   └── app.py                     # Streamlit 4 páginas
│
└── docker/
    ├── Dockerfile                 # Airflow + Java + pyspark + dbt
    └── docker-compose.yml         # Airflow + Postgres
```

---

## O que cada operador da DAG demonstra

| Task | Operador | Conceito |
|---|---|---|
| `fetch_*` | `PythonOperator` | Tasks paralelas dentro do TaskGroup |
| `validate_*` | `ShortCircuitOperator` | Falha graceful — pula bronze sem derrubar o pipeline |
| `bronze_*` | `PythonOperator` | Chama PySpark como subprocess |
| `silver_unify` | `PythonOperator` | Fan-in de 3 grupos + ACID MERGE com deduplicação |
| `bridge_to_duckdb` | `PythonOperator` | Delta → Parquet → DuckDB |
| `dbt_run` | `BashOperator` | Materializa gold layer |
| `dbt_test` | `BashOperator` | `trigger_rule=all_done` — roda mesmo com warnings |
| `notify_success` | `PythonOperator` | XCom para coletar métricas das tasks anteriores |

---

## Stack

| Componente | Tecnologia | Papel |
|---|---|---|
| Armazenamento | Delta Lake 3.2 | ACID, Time Travel, Schema Evolution |
| Processamento | PySpark 3.5 | Bronze → Silver, MERGE, particionamento |
| Transformação | dbt-duckdb 1.8 | Gold layer com SQL + testes de qualidade |
| Query Engine | DuckDB | Lê Parquet, serve dbt e dashboard |
| Orquestração | Airflow 2.9 | DAG com TaskGroups, ShortCircuit, XCom |
| Dashboard | Streamlit + Plotly | 4 páginas sobre o gold layer |
| CI/CD | GitHub Actions | dbt parse → run → test a cada push |
