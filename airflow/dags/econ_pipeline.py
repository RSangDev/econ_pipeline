"""
airflow/dags/econ_pipeline.py

Pipeline Econômico Brasileiro — DAG principal

Estrutura com TaskGroups e dependências complexas:

  [TaskGroup: ibge]  ──────────────────────────────┐
    fetch_ibge_pib                                  │
    fetch_ibge_populacao                            │
    validate_ibge                                   ├──► [TaskGroup: gold]
                                                    │      bridge_to_duckdb
  [TaskGroup: bcb]   ──────────────────────────────┤      dbt_run
    fetch_bcb_selic                                 │      dbt_test
    fetch_bcb_ipca                                  │    ──► notify_success
    fetch_bcb_cambio                                │
    validate_bcb                                    │
                                                    │
  [TaskGroup: ipea]  ──────────────────────────────┘
    fetch_ipea_desemprego
    fetch_ipea_gini
    validate_ipea

  ─── BRONZE (paralelo por fonte) ────────────────────
  bronze_ibge  bronze_bcb  bronze_ipea
        └──────────┴──────────┘
                   │
              silver_unify  (ACID MERGE)
                   │
              [TaskGroup: gold]
"""

from __future__ import annotations

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

RAW_DIR  = PROJECT_ROOT / "data" / "raw"
DBT_DIR  = PROJECT_ROOT / "dbt_project"
PYTHON   = sys.executable

logger = logging.getLogger(__name__)

# ── Configurações padrão ──────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "engenharia_dados",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


# ════════════════════════════════════════════════════════════════
# FUNÇÕES DAS TASKS
# ════════════════════════════════════════════════════════════════

def _fetch_and_save(func_name: str, **kwargs) -> str:
    """Executa uma função de fetch e salva o CSV em data/raw/."""
    import importlib
    from ingestion.apis import (
        fetch_ibge_pib, fetch_ibge_populacao,
        fetch_bcb_selic, fetch_bcb_ipca, fetch_bcb_cambio,
        fetch_ipea_desemprego, fetch_ipea_gini,
    )
    funcs = {
        "fetch_ibge_pib":        fetch_ibge_pib,
        "fetch_ibge_populacao":  fetch_ibge_populacao,
        "fetch_bcb_selic":       fetch_bcb_selic,
        "fetch_bcb_ipca":        fetch_bcb_ipca,
        "fetch_bcb_cambio":      fetch_bcb_cambio,
        "fetch_ipea_desemprego": fetch_ipea_desemprego,
        "fetch_ipea_gini":       fetch_ipea_gini,
    }
    nome = func_name.replace("fetch_", "")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    df = funcs[func_name]()
    path = RAW_DIR / f"{nome}.csv"
    df.to_csv(path, index=False)
    logger.info(f"Salvo: {path} ({len(df)} registros)")

    # Pusha métricas para o XCom
    ti = kwargs["ti"]
    ti.xcom_push(key=f"n_{nome}", value=len(df))
    return str(path)


def _validate_fonte(fonte: str, min_records: int = 10, **kwargs) -> bool:
    """
    Valida se os arquivos da fonte foram gerados com dados suficientes.
    ShortCircuitOperator: retorna False → pula tasks downstream.
    """
    import pandas as pd
    arquivos = list(RAW_DIR.glob(f"{fonte}_*.csv"))
    if not arquivos:
        logger.error(f"Nenhum arquivo encontrado para fonte: {fonte}")
        return False
    total = 0
    for f in arquivos:
        try:
            n = len(pd.read_csv(f))
            total += n
            logger.info(f"  {f.name}: {n} registros")
        except Exception as e:
            logger.error(f"  {f.name}: erro ao ler — {e}")
            return False
    if total < min_records:
        logger.error(f"Total insuficiente para {fonte}: {total} < {min_records}")
        return False
    logger.info(f"✅ {fonte}: {total} registros validados")
    return True


def _run_bronze(fontes: list[str], **kwargs):
    """Executa o job PySpark de Bronze para as fontes especificadas."""
    import subprocess
    bronze_script = str(PROJECT_ROOT / "spark_jobs" / "bronze.py")
    cmd = [PYTHON, bronze_script] + fontes
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise RuntimeError(f"bronze.py falhou:\n{result.stderr}")
    logger.info(result.stdout)


def _run_silver(**kwargs):
    """Executa o job PySpark de Silver (ACID MERGE)."""
    import subprocess
    script = str(PROJECT_ROOT / "spark_jobs" / "silver.py")
    result = subprocess.run([PYTHON, script], capture_output=True, text=True,
                            cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise RuntimeError(f"silver.py falhou:\n{result.stderr}")
    logger.info(result.stdout)


def _run_bridge(**kwargs):
    """Delta Lake → Parquet → DuckDB."""
    import subprocess
    script = str(PROJECT_ROOT / "spark_jobs" / "bridge.py")
    result = subprocess.run([PYTHON, script], capture_output=True, text=True,
                            cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise RuntimeError(f"bridge.py falhou:\n{result.stderr}")
    logger.info(result.stdout)


def _notify_success(**kwargs):
    """
    Notificação de sucesso — imprime resumo do pipeline.
    Em produção: substituir por Slack webhook, email ou PagerDuty.
    """
    ti  = kwargs["ti"]
    dag = kwargs["dag"]

    # Coleta métricas dos XComs
    metricas = {}
    for task_id in ["ibge.fetch_ibge_pib", "bcb.fetch_bcb_selic",
                    "ipea.fetch_ipea_desemprego"]:
        try:
            val = ti.xcom_pull(task_ids=task_id)
            if val:
                metricas[task_id] = val
        except Exception:
            pass

    msg = f"""
╔══════════════════════════════════════════════╗
║     Pipeline Econômico — Execução concluída  ║
╠══════════════════════════════════════════════╣
║  DAG:         {dag.dag_id:<30} ║
║  Execução:    {kwargs['ts']:<30} ║
║  Status:      ✅ SUCESSO                      ║
╚══════════════════════════════════════════════╝
    """
    logger.info(msg)
    print(msg)


# ════════════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════════════
with DAG(
    dag_id="econ_pipeline_brasil",
    default_args=DEFAULT_ARGS,
    description="Pipeline de indicadores econômicos BR — IBGE + BCB + IPEA",
    schedule_interval="0 6 * * *",    # diário às 06h
    catchup=False,
    max_active_runs=1,
    tags=["economia", "ibge", "bcb", "ipea", "delta-lake", "dbt"],
) as dag:

    # ── TaskGroup: IBGE ─────────────────────────────────────────
    with TaskGroup("ibge", tooltip="Ingestão IBGE — PIB e população") as tg_ibge:

        fetch_pib = PythonOperator(
            task_id="fetch_ibge_pib",
            python_callable=_fetch_and_save,
            op_kwargs={"func_name": "fetch_ibge_pib"},
        )

        fetch_pop = PythonOperator(
            task_id="fetch_ibge_populacao",
            python_callable=_fetch_and_save,
            op_kwargs={"func_name": "fetch_ibge_populacao"},
        )

        validate_ibge = ShortCircuitOperator(
            task_id="validate_ibge",
            python_callable=_validate_fonte,
            op_kwargs={"fonte": "ibge"},
        )

        bronze_ibge = PythonOperator(
            task_id="bronze_ibge",
            python_callable=_run_bronze,
            op_kwargs={"fontes": ["ibge_pib", "ibge_populacao"]},
        )

        # Fetch em paralelo → validate → bronze
        [fetch_pib, fetch_pop] >> validate_ibge >> bronze_ibge

    # ── TaskGroup: BCB ──────────────────────────────────────────
    with TaskGroup("bcb", tooltip="Ingestão Banco Central — SELIC, IPCA, câmbio") as tg_bcb:

        fetch_selic = PythonOperator(
            task_id="fetch_bcb_selic",
            python_callable=_fetch_and_save,
            op_kwargs={"func_name": "fetch_bcb_selic"},
        )

        fetch_ipca = PythonOperator(
            task_id="fetch_bcb_ipca",
            python_callable=_fetch_and_save,
            op_kwargs={"func_name": "fetch_bcb_ipca"},
        )

        fetch_cambio = PythonOperator(
            task_id="fetch_bcb_cambio",
            python_callable=_fetch_and_save,
            op_kwargs={"func_name": "fetch_bcb_cambio"},
        )

        validate_bcb = ShortCircuitOperator(
            task_id="validate_bcb",
            python_callable=_validate_fonte,
            op_kwargs={"fonte": "bcb"},
        )

        bronze_bcb = PythonOperator(
            task_id="bronze_bcb",
            python_callable=_run_bronze,
            op_kwargs={"fontes": ["bcb_selic", "bcb_ipca", "bcb_cambio"]},
        )

        [fetch_selic, fetch_ipca, fetch_cambio] >> validate_bcb >> bronze_bcb

    # ── TaskGroup: IPEA ─────────────────────────────────────────
    with TaskGroup("ipea", tooltip="Ingestão IPEA — desemprego, renda") as tg_ipea:

        fetch_desemp = PythonOperator(
            task_id="fetch_ipea_desemprego",
            python_callable=_fetch_and_save,
            op_kwargs={"func_name": "fetch_ipea_desemprego"},
        )

        fetch_gini = PythonOperator(
            task_id="fetch_ipea_gini",
            python_callable=_fetch_and_save,
            op_kwargs={"func_name": "fetch_ipea_gini"},
        )

        validate_ipea = ShortCircuitOperator(
            task_id="validate_ipea",
            python_callable=_validate_fonte,
            op_kwargs={"fonte": "ipea"},
        )

        bronze_ipea = PythonOperator(
            task_id="bronze_ipea",
            python_callable=_run_bronze,
            op_kwargs={"fontes": ["ipea_desemprego", "ipea_gini"]},
        )

        [fetch_desemp, fetch_gini] >> validate_ipea >> bronze_ipea

    # ── Silver: ACID MERGE (espera TODOS os bronzes) ─────────────
    silver_task = PythonOperator(
        task_id="silver_unify",
        python_callable=_run_silver,
        doc_md="""
        ## Silver — Unificação com ACID MERGE
        Aguarda bronze de IBGE, BCB e IPEA.
        Une todas as fontes num schema comum e executa MERGE no Delta Lake.
        """,
    )

    # ── TaskGroup: Gold ─────────────────────────────────────────
    with TaskGroup("gold", tooltip="Gold layer — bridge + dbt") as tg_gold:

        bridge = PythonOperator(
            task_id="bridge_to_duckdb",
            python_callable=_run_bridge,
        )

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                f"cd {DBT_DIR} && "
                f"dbt run --profiles-dir . --project-dir . --no-partial-parse"
            ),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                f"cd {DBT_DIR} && "
                f"dbt test --profiles-dir . --project-dir ."
            ),
            trigger_rule="all_done",  # roda mesmo se testes falharem (para ver o report)
        )

        bridge >> dbt_run >> dbt_test

    # ── Notificação final ────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        trigger_rule="all_success",
    )

    # ════════════════════════════════════════════════════════════
    # GRAFO DE DEPENDÊNCIAS
    # Os 3 TaskGroups rodam em paralelo.
    # Silver só começa quando TODOS os bronzes terminam.
    # Gold só começa quando Silver confirmar sucesso.
    # ════════════════════════════════════════════════════════════
    [tg_ibge, tg_bcb, tg_ipea] >> silver_task >> tg_gold >> notify
