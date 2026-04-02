"""
spark_jobs/spark_session.py
Fábrica de SparkSession com Delta Lake.

Lógica:
- Se /opt/spark-jars existir (Docker), usa JARs locais — sem Ivy
- Se não existir (local/Windows), usa configure_spark_with_delta_pip
"""
import os
from pyspark.sql import SparkSession

_JARS_DIR  = "/opt/spark-jars"
_JAR_DELTA = os.path.join(_JARS_DIR, "delta-spark_2.12-3.2.0.jar")
_JAR_STORE = os.path.join(_JARS_DIR, "delta-storage-3.2.0.jar")


def get_spark(app_name: str = "econ_spark") -> SparkSession:
    if os.path.exists(_JAR_DELTA) and os.path.exists(_JAR_STORE):
        # Docker: JARs já estão em /opt/spark-jars — sem Ivy
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.jars", f"{_JAR_DELTA},{_JAR_STORE}")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
    else:
        # Local/Windows: usa configure_spark_with_delta_pip (baixa via Ivy)
        from delta import configure_spark_with_delta_pip
        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark