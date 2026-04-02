"""
airflow/dags/econ_pipeline.py
Pipeline Econômico Brasileiro — DAG principal com TaskGroups
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

logger = logging.getLogger(__name__)

# ── Resolve PROJECT_ROOT de forma robusta ───────────────────────
# Suporta variável de ambiente como override explícito.
# Fallback: a DAG fica em <ROOT>/airflow/dags/, então sobe 3 níveis.
# Resolve PROJECT_ROOT em ordem de prioridade:
# 1. Variável de ambiente explícita (definida no docker-compose)
# 2. /opt/project (path padrão do volume no Docker)
# 3. Cálculo baseado em __file__ (fallback local)
def _resolve_root() -> Path:
    env_val = os.environ.get("ECON_PROJECT_ROOT", "").strip()
    if env_val and Path(env_val).is_dir():
        return Path(env_val)
    docker_default = Path("/opt/project")
    if docker_default.is_dir():
        return docker_default
    # Fallback: sobe 3 níveis a partir de airflow/dags/dag.py
    return Path(os.path.abspath(__file__)).parent.parent.parent

PROJECT_ROOT = _resolve_root()

RAW_DIR = PROJECT_ROOT / "data" / "raw"
DBT_DIR = PROJECT_ROOT / "dbt_project"
IS_WIN  = sys.platform == "win32"


def _find_python() -> str:
    """
    Encontra o Python que tem o pyspark instalado.
    No Docker do Airflow, pyspark é instalado via pip no airflow-init,
    mas sys.executable pode apontar para um Python diferente dependendo
    de como a task é iniciada.
    
    Ordem de busca:
    1. PYSPARK_PYTHON env var (configurável no compose)
    2. Qualquer python3 no PATH que consiga importar pyspark
    3. sys.executable como último recurso
    """
    # Override explícito via variável de ambiente
    env_python = os.environ.get("PYSPARK_PYTHON", "").strip()
    if env_python and Path(env_python).exists():
        return env_python

    # Testa candidatos em ordem de prioridade
    import subprocess as _sp
    candidatos = [
        "/home/airflow/.local/bin/python3",   # pip install como user airflow
        "/home/airflow/.local/bin/python",
        sys.executable,
        "/usr/local/bin/python3",
        "/usr/bin/python3",
        "python3",
        "python",
    ]
    for py in candidatos:
        try:
            r = _sp.run(
                [py, "-c", "import pyspark; print('ok')"],
                capture_output=True, text=True, timeout=5
            )
            if r.returncode == 0 and "ok" in r.stdout:
                return py
        except Exception:
            continue

    # Fallback
    return sys.executable


def _dbt_bin() -> str:
    exe  = "dbt.exe" if IS_WIN else "dbt"
    # Tenta no mesmo diretório do Python encontrado, depois no PATH
    for base in [Path(PYTHON).parent, Path("/home/airflow/.local/bin")]:
        full = base / exe
        if full.exists():
            return str(full)
    return "dbt"


PYTHON = _find_python()
DBT    = _dbt_bin()


def _setup_path() -> Path:
    """
    Injeta PROJECT_ROOT no sys.path do processo worker.
    Precisa ser chamado no início de cada função de task porque o
    LocalExecutor cria processos filhos que não herdam o sys.path
    modificado durante o parse da DAG.
    """
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    return PROJECT_ROOT


# ════════════════════════════════════════════════════════════════
# FUNÇÕES DAS TASKS
# ════════════════════════════════════════════════════════════════

def _fetch_and_save(func_name: str, **kwargs) -> str:
    root = _setup_path()

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

    nome    = func_name.replace("fetch_", "")
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    df = funcs[func_name]()
    path = raw_dir / f"{nome}.csv"
    df.to_csv(path, index=False)

    n = len(df)
    if n == 0:
        logger.warning(f"API retornou 0 registros para {func_name} — CSV vazio salvo, validate decidirá")
    else:
        logger.info(f"Salvo: {path} ({n} registros)")
    kwargs["ti"].xcom_push(key=f"n_{nome}", value=n)
    return str(path)


def _validate_fonte(fonte: str, min_records: int = 10, **kwargs) -> bool:
    """
    Valida se pelo menos um CSV da fonte tem dados suficientes.
    CSVs individuais vazios são tolerados — só falha se o total for zero.
    ShortCircuitOperator: False → pula tasks downstream sem marcar como falha.
    """
    import pandas as pd
    root    = _setup_path()
    raw_dir = root / "data" / "raw"

    arquivos = list(raw_dir.glob(f"{fonte}_*.csv"))
    if not arquivos:
        logger.error(f"Nenhum CSV encontrado para '{fonte}' em {raw_dir}")
        return False

    total = 0
    for f in arquivos:
        try:
            df = pd.read_csv(f)
            n  = len(df)
            total += n
            if n == 0:
                logger.warning(f"  {f.name}: vazio (tolerado)")
            else:
                logger.info(f"  {f.name}: {n} registros OK")
        except Exception as e:
            # CSV malformado — loga mas não aborta, outros arquivos podem compensar
            logger.warning(f"  {f.name}: erro ao ler ({e}) — ignorando este arquivo")

    if total < min_records:
        logger.error(f"Total insuficiente para '{fonte}': {total} < {min_records}")
        return False

    logger.info(f"✅ {fonte}: {total} registros validados")
    return True


def _run_spark_job(script_name: str, extra_args: list = None, **kwargs):
    """
    Executa um job PySpark como subprocess.
    Passa PYTHONPATH e PROJECT_ROOT explicitamente para o processo filho.
    stdout+stderr são capturados juntos e logados no Airflow UI.
    """
    import subprocess
    root   = _setup_path()
    script = str(root / "spark_jobs" / script_name)
    cmd    = [PYTHON, script] + (extra_args or [])

    logger.info(f"Executando: {' '.join(cmd)}")
    logger.info(f"CWD: {root}")

    env = {
        **os.environ,
        "PYTHONPATH":          str(root),
        "ECON_PROJECT_ROOT":   str(root),
    }

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(root),
        env=env,
    )

    for line in result.stdout.splitlines():
        logger.info(line)

    if result.returncode != 0:
        raise RuntimeError(
            f"{script_name} encerrou com código {result.returncode}. "
            f"Veja o log acima para detalhes."
        )


def _run_bronze(fontes: list, **kwargs):
    _run_spark_job("bronze.py", extra_args=fontes, **kwargs)


def _run_silver(**kwargs):
    _run_spark_job("silver.py", **kwargs)


def _run_bridge(**kwargs):
    _run_spark_job("bridge.py", **kwargs)


def _notify_success(**kwargs):
    _setup_path()
    ti  = kwargs["ti"]
    dag = kwargs["dag"]

    metricas = {}
    for task_id in ["ibge.fetch_ibge_pib", "bcb.fetch_bcb_selic",
                    "ipea.fetch_ipea_desemprego"]:
        try:
            val = ti.xcom_pull(task_ids=task_id)
            if val:
                metricas[task_id] = val
        except Exception:
            pass

    msg = (
        f"\n{'='*50}\n"
        f"Pipeline Econômico — CONCLUÍDO\n"
        f"DAG:    {dag.dag_id}\n"
        f"Run:    {kwargs['ts']}\n"
        f"Status: SUCESSO\n"
        f"{'='*50}"
    )
    logger.info(msg)
    print(msg)


# ════════════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════════════
with DAG(
    dag_id="econ_pipeline_brasil",
    default_args={
        "owner":             "engenharia_dados",
        "depends_on_past":   False,
        "start_date":        datetime(2024, 1, 1),
        "retries":           0,
        "execution_timeout": timedelta(minutes=45),
    },
    description="Pipeline de indicadores econômicos BR — IBGE + BCB + IPEA",
    schedule_interval="0 6 * * *",
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

    # ── Silver ───────────────────────────────────────────────────
    silver_task = PythonOperator(
        task_id="silver_unify",
        python_callable=_run_silver,
    )

    # ── TaskGroup: Gold ─────────────────────────────────────────
    with TaskGroup("gold", tooltip="Gold layer — bridge + dbt") as tg_gold:

        bridge = PythonOperator(
            task_id="bridge_to_duckdb",
            python_callable=_run_bridge,
        )
        # PATH explícito garante que dbt seja encontrado independente de onde foi instalado
        _dbt_path = "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:" + os.environ.get("PATH","")
        _dbt_env  = {**os.environ, "PATH": _dbt_path, "PYTHONPATH": str(PROJECT_ROOT)}

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                "export PATH=/home/airflow/.local/bin:/usr/local/bin:/usr/bin:$PATH && "
                f"cd '{DBT_DIR}' && "
                "dbt run --profiles-dir . --project-dir . --no-partial-parse"
            ),
            env=_dbt_env,
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                "export PATH=/home/airflow/.local/bin:/usr/local/bin:/usr/bin:$PATH && "
                f"cd '{DBT_DIR}' && "
                "dbt test --profiles-dir . --project-dir ."
            ),
            env=_dbt_env,
            trigger_rule="all_done",
        )
        bridge >> dbt_run >> dbt_test

    # ── Notificação ──────────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        trigger_rule="all_success",
    )

    # ── Grafo de dependências ────────────────────────────────────
    [tg_ibge, tg_bcb, tg_ipea] >> silver_task >> tg_gold >> notify