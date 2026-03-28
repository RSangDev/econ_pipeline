"""
spark_jobs/bronze.py
Ingere os CSVs das APIs no Delta Lake Bronze.
Cada fonte tem seu próprio path particionado por fonte/ano.
"""
import os, sys, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _winutils import setup; setup()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta import configure_spark_with_delta_pip

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR   = os.path.join(ROOT, "data", "raw")
DELTA_DIR = os.path.join(ROOT, "data", "delta")


def get_spark():
    builder = (
        SparkSession.builder.appName("econ_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Schema base para séries temporais (BCB, IPEA)
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

# Schema para PIB (trimestral)
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
    spark = get_spark()
    fontes = fontes or list(FONTES.keys())

    for fonte in fontes:
        csv_path = os.path.join(RAW_DIR, f"{fonte}.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"Arquivo não encontrado: {csv_path}")
            continue

        schema, grupo = FONTES[fonte]
        delta_path = os.path.join(DELTA_DIR, "bronze", grupo)

        logger.info(f"Bronze: {fonte} → {delta_path}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", schema is None)
            .csv(csv_path) if schema is None
            else spark.read.option("header", "true").schema(schema).csv(csv_path)
        )

        df = (df
            .withColumn("_source",      F.lit(fonte))
            .withColumn("_loaded_at",   F.current_timestamp())
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
