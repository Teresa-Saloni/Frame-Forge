FROM apache/airflow:2.10.2-python3.11

USER root
# Java + spark-submit client so SparkSubmitOperator can talk to the standalone cluster
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.3
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin
# build context is the repo root (see docker-compose.yml)
COPY --chown=airflow:root spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER airflow
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
