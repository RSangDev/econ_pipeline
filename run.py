"""
run.py — Runner local (sem Airflow) para desenvolvimento e testes.

Em produção: a DAG do Airflow orquestra os mesmos passos.
Aqui: executamos manualmente para testar o pipeline completo.

Uso:
  python run.py                      # pipeline completo + dashboard
  python run.py --only-pipeline      # só pipeline
  python run.py --only-dashboard     # só dashboard
  python run.py --skip-spark         # pula Spark (Delta já existe)
  python run.py --fontes bcb ipea    # só fontes específicas
"""

import argparse
import os
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT   = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable
IS_WIN = sys.platform == "win32"


def _bin(name: str) -> str:
    exe  = name + (".exe" if IS_WIN else "")
    full = os.path.join(os.path.dirname(PYTHON), exe)
    return full if os.path.exists(full) else name


DBT       = _bin("dbt")
STREAMLIT = _bin("streamlit")
DBT_DIR   = os.path.join(ROOT, "dbt_project")

TODAS_FONTES = [
    "ibge_pib", "ibge_populacao",
    "bcb_selic", "bcb_ipca", "bcb_cambio",
    "ipea_desemprego", "ipea_gini",
]


def run_cmd(cmd: list, cwd: str = ROOT, label: str = "") -> bool:
    label = label or os.path.basename(cmd[1] if len(cmd) > 1 else cmd[0])
    logger.info(f"▶ {label}")
    r = subprocess.run(cmd, cwd=cwd)
    if r.returncode != 0:
        logger.error(f"✗ {label} (código {r.returncode})")
        return False
    logger.info(f"✓ {label}")
    return True


def pipeline(fontes: list, skip_spark: bool = False) -> bool:
    # ── 1. Ingestão das APIs ──────────────────────────────────
    logger.info("=" * 55)
    logger.info("STEP 1/5 — Ingestão das APIs (IBGE + BCB + IPEA)")
    logger.info("=" * 55)
    sys.path.insert(0, ROOT)
    from ingestion.apis import (
        fetch_ibge_pib, fetch_ibge_populacao,
        fetch_bcb_selic, fetch_bcb_ipca, fetch_bcb_cambio,
        fetch_ipea_desemprego, fetch_ipea_gini,
    )
    import pandas as pd

    func_map = {
        "ibge_pib":        fetch_ibge_pib,
        "ibge_populacao":  fetch_ibge_populacao,
        "bcb_selic":       fetch_bcb_selic,
        "bcb_ipca":        fetch_bcb_ipca,
        "bcb_cambio":      fetch_bcb_cambio,
        "ipea_desemprego": fetch_ipea_desemprego,
        "ipea_gini":       fetch_ipea_gini,
    }
    os.makedirs(os.path.join(ROOT, "data", "raw"), exist_ok=True)
    for nome in fontes:
        try:
            df = func_map[nome]()
            path = os.path.join(ROOT, "data", "raw", f"{nome}.csv")
            df.to_csv(path, index=False)
            logger.info(f"  ✓ {nome}: {len(df)} registros")
        except Exception as e:
            logger.error(f"  ✗ {nome}: {e}")

    if not skip_spark:
        # ── 2. Bronze ────────────────────────────────────────
        logger.info("=" * 55)
        logger.info("STEP 2/5 — Bronze (PySpark → Delta Lake)")
        logger.info("=" * 55)
        if not run_cmd([PYTHON, "spark_jobs/bronze.py"] + fontes, label="bronze.py"):
            return False

        # ── 3. Silver ────────────────────────────────────────
        logger.info("=" * 55)
        logger.info("STEP 3/5 — Silver (ACID MERGE)")
        logger.info("=" * 55)
        if not run_cmd([PYTHON, "spark_jobs/silver.py"], label="silver.py"):
            return False

        # ── 4. Bridge ────────────────────────────────────────
        logger.info("=" * 55)
        logger.info("STEP 4/5 — Bridge Delta → DuckDB")
        logger.info("=" * 55)
        if not run_cmd([PYTHON, "spark_jobs/bridge.py"], label="bridge.py"):
            return False

    # ── 5. dbt Gold ──────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("STEP 5/5 — dbt run + dbt test")
    logger.info("=" * 55)
    if not run_cmd([DBT, "deps"], cwd=DBT_DIR, label="dbt deps"):
        return False
    if not run_cmd([DBT, "run",  "--profiles-dir", ".", "--project-dir", "."], cwd=DBT_DIR, label="dbt run"):
        return False
    run_cmd([DBT, "test", "--profiles-dir", ".", "--project-dir", "."], cwd=DBT_DIR, label="dbt test")

    logger.info("Pipeline concluído.")
    return True


def dashboard():
    logger.info("Iniciando dashboard → http://localhost:8501")
    subprocess.run([STREAMLIT, "run", "dashboard/app.py"], cwd=ROOT)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--only-pipeline",  action="store_true")
    p.add_argument("--only-dashboard", action="store_true")
    p.add_argument("--skip-spark",     action="store_true")
    p.add_argument("--fontes", nargs="+", default=TODAS_FONTES)
    args = p.parse_args()

    if args.only_dashboard:
        dashboard()
    elif args.only_pipeline:
        pipeline(args.fontes, skip_spark=args.skip_spark)
    else:
        ok = pipeline(args.fontes, skip_spark=args.skip_spark)
        if ok:
            dashboard()
