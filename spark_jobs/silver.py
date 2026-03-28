"""
spark_jobs/silver.py
Transforma Bronze → Silver com ACID MERGE.
Cada indicador é limpo, enriquecido e unificado num schema comum.
"""
import os, sys, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _winutils import setup; setup()
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_DIR = os.path.join(ROOT, "data", "delta")
SILVER_PATH = os.path.join(DELTA_DIR, "silver", "indicadores")


def get_spark():
    builder = (
        SparkSession.builder.appName("econ_silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_silver(spark: SparkSession) -> DataFrame:
    """Unifica todas as fontes Bronze num schema Silver comum."""
    frames = []

    # ── BCB series ────────────────────────────────────────────
    bcb_path = os.path.join(DELTA_DIR, "bronze", "series_bcb")
    if DeltaTable.isDeltaTable(spark, bcb_path):
        bcb = spark.read.format("delta").load(bcb_path)
        bcb = (bcb
            .filter(F.col("valor").isNotNull())
            .withColumn("categoria", F.lit("monetario"))
            .withColumn("chave", F.concat_ws("_", F.col("indicador"), F.col("data")))
            .select("chave", "fonte", "indicador", "categoria",
                    F.col("data").alias("periodo"),
                    "ano", "mes",
                    F.lit(None).cast("int").alias("trimestre"),
                    "valor", "unidade",
                    F.current_timestamp().alias("_silver_at"))
        )
        frames.append(bcb)
        logger.info(f"  BCB: {bcb.count()} registros")

    # ── IPEA series ───────────────────────────────────────────
    ipea_path = os.path.join(DELTA_DIR, "bronze", "series_ipea")
    if DeltaTable.isDeltaTable(spark, ipea_path):
        ipea = spark.read.format("delta").load(ipea_path)
        ipea = (ipea
            .filter(F.col("valor").isNotNull())
            .withColumn("categoria", F.lit("social"))
            .withColumn("chave", F.concat_ws("_", F.col("indicador"), F.col("data")))
            .select("chave", "fonte", "indicador", "categoria",
                    F.col("data").alias("periodo"),
                    "ano", "mes",
                    F.lit(None).cast("int").alias("trimestre"),
                    "valor", "unidade",
                    F.current_timestamp().alias("_silver_at"))
        )
        frames.append(ipea)
        logger.info(f"  IPEA: {ipea.count()} registros")

    # ── IBGE PIB ──────────────────────────────────────────────
    pib_path = os.path.join(DELTA_DIR, "bronze", "pib")
    if DeltaTable.isDeltaTable(spark, pib_path):
        pib = spark.read.format("delta").load(pib_path)
        pib = (pib
            .filter(F.col("valor").isNotNull())
            .withColumn("categoria", F.lit("atividade"))
            .withColumn("mes", F.lit(None).cast("int"))
            .withColumn("chave", F.concat_ws("_", F.col("indicador"), F.col("periodo")))
            .select("chave", "fonte", "indicador", "categoria",
                    "periodo", "ano", "mes", "trimestre",
                    "valor", "unidade",
                    F.current_timestamp().alias("_silver_at"))
        )
        frames.append(pib)
        logger.info(f"  IBGE PIB: {pib.count()} registros")

    if not frames:
        raise RuntimeError("Nenhuma fonte Bronze encontrada. Rode o Bronze primeiro.")

    # Union de todos os frames
    silver = frames[0]
    for f in frames[1:]:
        # Alinha colunas antes do union
        for col in silver.columns:
            if col not in f.columns:
                f = f.withColumn(col, F.lit(None))
        silver = silver.unionByName(f, allowMissingColumns=True)

    # Enriquecimento: variação YoY por indicador
    window_yoy = Window.partitionBy("indicador", "mes").orderBy("ano")
    silver = silver.withColumn(
        "variacao_yoy",
        F.round(
            (F.col("valor") - F.lag("valor").over(window_yoy))
            / F.abs(F.lag("valor").over(window_yoy)) * 100, 2
        )
    )

    return silver


def run_silver():
    spark = get_spark()
    logger.info("Construindo Silver layer...")

    silver = build_silver(spark)
    n = silver.count()
    logger.info(f"  → {n} registros unificados")

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info("MERGE (ACID transaction)...")
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(silver.alias("s"), "t.chave = s.chave")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            silver.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .partitionBy("fonte", "ano")
            .save(SILVER_PATH)
        )
        logger.info(f"  → Silver criado: {SILVER_PATH}")

    spark.stop()
    logger.info("Silver concluído.")


if __name__ == "__main__":
    run_silver()
