"""
spark_jobs/silver.py
Transforma Bronze → Silver com ACID MERGE.
"""
import os, sys, logging

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_session import get_spark

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT        = _ROOT
DELTA_DIR   = os.path.join(ROOT, "data", "delta")
SILVER_PATH = os.path.join(DELTA_DIR, "silver", "indicadores")


def build_silver(spark) -> DataFrame:
    frames = []

    for path_key, categoria in [
        ("series_bcb",  "monetario"),
        ("series_ipea", "social"),
    ]:
        path = os.path.join(DELTA_DIR, "bronze", path_key)
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        df = spark.read.format("delta").load(path)
        df = (df
            .filter(F.col("valor").isNotNull())
            .withColumn("categoria", F.lit(categoria))
            .withColumn("chave", F.concat_ws("_", F.col("indicador"), F.col("data")))
            .select("chave", "fonte", "indicador", "categoria",
                    F.col("data").alias("periodo"), "ano", "mes",
                    F.lit(None).cast("int").alias("trimestre"),
                    "valor", "unidade",
                    F.current_timestamp().alias("_silver_at"))
        )
        n = df.count()
        frames.append(df)
        logger.info(f"  {path_key}: {n} registros")

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
        logger.info(f"  pib: {pib.count()} registros")

    if not frames:
        raise RuntimeError("Nenhuma fonte Bronze encontrada.")

    silver = frames[0]
    for f in frames[1:]:
        for col in silver.columns:
            if col not in f.columns:
                f = f.withColumn(col, F.lit(None))
        silver = silver.unionByName(f, allowMissingColumns=True)

    # ── Deduplica pelo campo chave ─────────────────────────────────
    # O Bronze usa mode=append — múltiplas execuções acumulam linhas.
    # O MERGE do Delta falha com DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW
    # se a mesma chave aparecer mais de uma vez no source.
    # Solução: pega apenas a versão mais recente de cada chave.
    window_dedup = Window.partitionBy("chave").orderBy(F.col("_silver_at").desc())
    silver = (
        silver
        .withColumn("_rn", F.row_number().over(window_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Variação YoY
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
    spark  = get_spark("econ_silver")
    logger.info("Construindo Silver layer...")
    silver = build_silver(spark)
    n      = silver.count()
    logger.info(f"  → {n} registros após deduplicação")

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
        logger.info("  → MERGE concluído")
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