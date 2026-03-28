"""
spark_jobs/bridge.py
Exporta Silver Delta → Parquet → DuckDB view para o dbt consumir.
"""
import os, sys, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _winutils import setup; setup()
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_DIR    = os.path.join(ROOT, "data", "delta")
SILVER_PATH  = os.path.join(DELTA_DIR, "silver", "indicadores")
PARQUET_PATH = os.path.join(DELTA_DIR, "parquet", "silver_indicadores")
DW_PATH      = os.path.join(ROOT, "data", "warehouse", "econ.duckdb")


def get_spark():
    builder = (
        SparkSession.builder.appName("econ_bridge")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_bridge():
    # 1. Delta → Parquet
    spark = get_spark()
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
    conn = duckdb.connect(DW_PATH)
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
