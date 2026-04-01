FROM apache/airflow:2.9.1-python3.11

USER root

# Java para o PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Cria o diretório ivy2 com permissões corretas ANTES de qualquer uso do Spark
RUN mkdir -p /home/airflow/.ivy2/cache /home/airflow/.ivy2/jars \
    && chown -R airflow:airflow /home/airflow/.ivy2

# Pré-baixa os JARs do Delta Lake para evitar race condition do Ivy em runtime
RUN mkdir -p /opt/spark-jars \
    && curl -L -o /opt/spark-jars/delta-spark_2.12-3.2.0.jar \
       https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl -L -o /opt/spark-jars/delta-storage-3.2.0.jar \
       https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && chown -R airflow:airflow /opt/spark-jars

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt" \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    protobuf==4.25.3 \
    dbt-duckdb==1.8.1 \
    duckdb==0.10.3 \
    pandas==2.2.0 \
    requests==2.31.0

ENV PYSPARK_PYTHON="/home/airflow/.local/bin/python3"
ENV PYSPARK_DRIVER_PYTHON="/home/airflow/.local/bin/python3"
# Aponta o Spark para os JARs pré-baixados — bypassa o Ivy completamente
ENV SPARK_EXTRA_JARS="/opt/spark-jars/delta-spark_2.12-3.2.0.jar,/opt/spark-jars/delta-storage-3.2.0.jar"