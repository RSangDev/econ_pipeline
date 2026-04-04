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


def _normalize_serie(df, categoria: str) -> DataFrame:
    """Normaliza qualquer série para o schema Silver comum."""
    # Determina o campo de período — séries BCB têm 'data', PIB tem 'periodo'
    cols = df.columns
    if "data" in cols:
        periodo_col = F.col("data")
    elif "periodo" in cols:
        periodo_col = F.col("periodo")
    else:
        periodo_col = F.lit(None).cast("string")

    # mes pode não existir em séries anuais
    mes_col      = F.col("mes")      if "mes"      in cols else F.lit(None).cast("int")
    trimestre_col = F.col("trimestre") if "trimestre" in cols else F.lit(None).cast("int")

    return (df
        .filter(F.col("valor").isNotNull())
        .withColumn("categoria",  F.lit(categoria))
        .withColumn("periodo",    periodo_col)
        .withColumn("mes",        mes_col)
        .withColumn("trimestre",  trimestre_col)
        .withColumn("chave",      F.concat_ws("_", F.col("indicador"), periodo_col))
        .select("chave", "fonte", "indicador", "categoria",
                "periodo", "ano", "mes", "trimestre",
                "valor", "unidade",
                F.current_timestamp().alias("_silver_at"))
    )


def build_silver(spark) -> DataFrame:
    frames = []

    for path_key, categoria in [
        ("series_bcb",  "monetario"),
        ("series_ipea", "social"),
        ("pib",         "atividade"),   # PIB — pode vir do IBGE ou BCB fallback
    ]:
        path = os.path.join(DELTA_DIR, "bronze", path_key)
        if not DeltaTable.isDeltaTable(spark, path):
            logger.info(f"  {path_key}: não encontrado, pulando")
            continue
        df = spark.read.format("delta").load(path)
        n_raw = df.count()
        if n_raw == 0:
            logger.info(f"  {path_key}: vazio, pulando")
            continue

        df_norm = _normalize_serie(df, categoria)
        n = df_norm.count()
        frames.append(df_norm)
        logger.info(f"  {path_key}: {n_raw} → {n} registros normalizados")

    if not frames:
        raise RuntimeError("Nenhuma fonte Bronze encontrada.")

    silver = frames[0]
    for f in frames[1:]:
        for col in silver.columns:
            if col not in f.columns:
                f = f.withColumn(col, F.lit(None))
        silver = silver.unionByName(f, allowMissingColumns=True)

    # Deduplica — Bronze usa mode=append, chave pode repetir entre execuções
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
    spark = get_spark("econ_silver")
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