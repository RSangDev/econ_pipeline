"""
spark_jobs/spark_session.py
Fábrica de SparkSession com Delta Lake.

- Windows local: configura HADOOP_HOME via _winutils
- Docker (/opt/spark-jars existe): usa JARs pré-baixados, sem Ivy
- Fallback: configure_spark_with_delta_pip (baixa via Ivy)
"""
import os
import sys
import logging

logger = logging.getLogger(__name__)


def _setup_winutils():
    if sys.platform != "win32":
        return
    import urllib.request
    root    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bin_dir = os.path.join(root, "hadoop", "bin")
    os.makedirs(bin_dir, exist_ok=True)
    for name, url in [
        ("winutils.exe", "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"),
        ("hadoop.dll",   "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"),
    ]:
        dest = os.path.join(bin_dir, name)
        if not os.path.exists(dest):
            print(f"Baixando {name}...")
            urllib.request.urlretrieve(url, dest)
    os.environ["HADOOP_HOME"] = os.path.join(root, "hadoop")
    os.environ["PATH"]        = bin_dir + os.pathsep + os.environ.get("PATH", "")


_setup_winutils()

_JARS_DIR  = "/opt/spark-jars"
_JAR_DELTA = os.path.join(_JARS_DIR, "delta-spark_2.12-3.2.0.jar")
_JAR_STORE = os.path.join(_JARS_DIR, "delta-storage-3.2.0.jar")


def get_spark(app_name: str = "econ_spark"):
    from pyspark.sql import SparkSession

    # Garante que não há SparkContext órfão de execução anterior
    # Isso previne o NullPointerException no BlockManagerMasterEndpoint
    # que ocorre no Windows quando múltiplas sessões são criadas em sequência
    try:
        existing = SparkSession.getActiveSession()
        if existing is not None:
            existing.stop()
            # Pequena pausa para o JVM liberar recursos
            import time
            time.sleep(1)
    except Exception:
        pass

    if os.path.exists(_JAR_DELTA) and os.path.exists(_JAR_STORE):
        # Docker: JARs pré-baixados em /opt/spark-jars
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
            # Evita problemas de cleanup de temp dir no Windows
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.network.timeout", "120s")
            .getOrCreate()
        )
    else:
        # Local: usa Ivy para baixar Delta JARs
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
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.network.timeout", "120s")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark