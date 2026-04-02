"""
spark_jobs/bridge.py
Exporta Silver Delta → Parquet → DuckDB view para o dbt consumir.
"""
import os, sys, logging, shutil

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_session import get_spark

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT         = _ROOT
DELTA_DIR    = os.path.join(ROOT, "data", "delta")
SILVER_PATH  = os.path.join(DELTA_DIR, "silver", "indicadores")
PARQUET_PATH = os.path.join(DELTA_DIR, "parquet", "silver_indicadores")
DW_PATH      = os.path.join(ROOT, "data", "warehouse", "econ.duckdb")


def _safe_connect(path: str) -> duckdb.DuckDBPyConnection:
    """
    Conecta ao DuckDB. Se o arquivo existir mas estiver corrompido
    ou incompatível (SerializationException), deleta e recria.
    """
    if os.path.exists(path):
        try:
            conn = duckdb.connect(path)
            conn.execute("SELECT 1")  # testa se o arquivo é legível
            conn.close()
        except Exception as e:
            logger.warning(f"DuckDB existente inválido ({e}) — deletando e recriando")
            os.remove(path)
            # Remove também o arquivo WAL se existir
            wal = path + ".wal"
            if os.path.exists(wal):
                os.remove(wal)
    return duckdb.connect(path)


def run_bridge():
    # 1. Silver Delta → Parquet
    spark = get_spark("econ_bridge")
    logger.info("Silver Delta → Parquet...")
    df = spark.read.format("delta").load(SILVER_PATH)
    n  = df.count()
    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
    df.write.mode("overwrite").parquet(PARQUET_PATH)
    logger.info(f"  → {n} registros exportados")
    spark.stop()

    # 2. Parquet → DuckDB view
    logger.info("Registrando no DuckDB...")
    os.makedirs(os.path.dirname(DW_PATH), exist_ok=True)

    conn = _safe_connect(DW_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    parquet_glob = PARQUET_PATH.replace("\\", "/") + "/**/*.parquet"
    conn.execute(f"""
        CREATE OR REPLACE VIEW silver.indicadores AS
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    """)
    n_check = conn.execute("SELECT COUNT(*) FROM silver.indicadores").fetchone()[0]
    logger.info(f"  → DuckDB silver.indicadores: {n_check} registros")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.close()
    logger.info(f"Bridge concluída: {DW_PATH}")


if __name__ == "__main__":
    run_bridge()