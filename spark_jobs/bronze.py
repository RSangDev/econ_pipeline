"""
spark_jobs/bronze.py
Ingere os CSVs das APIs no Delta Lake Bronze.
"""
import os, sys, logging

# Garante que a raiz do projeto está no path antes de qualquer import local
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Import direto do arquivo — evita problemas de resolução de pacote
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_session import get_spark

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT      = _ROOT
RAW_DIR   = os.path.join(ROOT, "data", "raw")
DELTA_DIR = os.path.join(ROOT, "data", "delta")

SERIE_SCHEMA = StructType([
    StructField("fonte",     StringType(), True),
    StructField("indicador", StringType(), True),
    StructField("data",      StringType(), True),
    StructField("ano",       IntegerType(), True),
    StructField("mes",       IntegerType(), True),
    StructField("valor",     DoubleType(),  True),
    StructField("unidade",   StringType(), True),
    StructField("dt_carga",  StringType(), True),
])

PIB_SCHEMA = StructType([
    StructField("fonte",     StringType(), True),
    StructField("indicador", StringType(), True),
    StructField("periodo",   StringType(), True),
    StructField("ano",       IntegerType(), True),
    StructField("trimestre", IntegerType(), True),
    StructField("valor",     DoubleType(),  True),
    StructField("unidade",   StringType(), True),
    StructField("dt_carga",  StringType(), True),
])

FONTES = {
    "ibge_pib":        (PIB_SCHEMA,   "pib"),
    "ibge_populacao":  (None,         "populacao"),
    "bcb_selic":       (SERIE_SCHEMA, "series_bcb"),
    "bcb_ipca":        (SERIE_SCHEMA, "series_bcb"),
    "bcb_cambio":      (SERIE_SCHEMA, "series_bcb"),
    "ipea_desemprego": (SERIE_SCHEMA, "series_ipea"),
    "ipea_gini":       (SERIE_SCHEMA, "series_ipea"),
}


def ingest_bronze(fontes: list = None):
    spark  = get_spark("econ_bronze")
    fontes = fontes or list(FONTES.keys())

    for fonte in fontes:
        csv_path = os.path.join(RAW_DIR, f"{fonte}.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"Arquivo não encontrado: {csv_path}")
            continue

        schema, grupo = FONTES[fonte]
        delta_path    = os.path.join(DELTA_DIR, "bronze", grupo)

        logger.info(f"Bronze: {fonte} → {delta_path}")

        if schema is None:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        else:
            df = spark.read.option("header", "true").schema(schema).csv(csv_path)

        if df.count() == 0:
            logger.warning(f"  CSV vazio para {fonte}, pulando")
            continue

        df = (df
            .withColumn("_source",    F.lit(fonte))
            .withColumn("_loaded_at", F.current_timestamp())
        )

        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(delta_path)
        )
        logger.info(f"  → {df.count()} registros escritos")

    spark.stop()
    logger.info("Bronze concluído.")


if __name__ == "__main__":
    fontes = sys.argv[1:] or None
    ingest_bronze(fontes)