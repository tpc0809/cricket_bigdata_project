# Use the official Apache Airflow image as base
FROM apache/airflow:2.9.0

# Switch to root user for installations
USER root

# Set Non-Interactive mode to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# ✅ Install Required Dependencies
RUN apt-get update && apt-get install -y \
    curl wget unzip tar tini iputils-ping netcat-openbsd procps \
    && rm -rf /var/lib/apt/lists/*

# ✅ Install OpenJDK 11 (ARM64-Compatible)
RUN mkdir -p /usr/lib/jvm && \
    curl -fsSL -o /tmp/openjdk-11.tar.gz "https://api.adoptium.net/v3/binary/latest/11/ga/linux/aarch64/jdk/hotspot/normal/eclipse" && \
    tar -xzf /tmp/openjdk-11.tar.gz -C /usr/lib/jvm && \
    mv /usr/lib/jvm/jdk-* /usr/lib/jvm/java-11-openjdk && \
    rm /tmp/openjdk-11.tar.gz

# ✅ Set Java Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ✅ Install Spark (ARM64 Version Compatible with Python 3.10)
RUN mkdir -p /opt/spark && \
    wget -q -O /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz" && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    ls -l /opt/spark && \
    ls -l /opt/spark/bin && \
    rm /tmp/spark.tgz

# ✅ Ensure Spark Submit Works
RUN chmod -R 777 /opt/spark

# ✅ Set Spark Environment Variables
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ✅ Create Spark JARs Directory
RUN mkdir -p /opt/spark/jars

# ✅ Install PostgreSQL JDBC Driver
RUN wget -O /opt/spark/jars/postgresql-42.3.1.jar "https://jdbc.postgresql.org/download/postgresql-42.3.1.jar"

# ✅ Switch to Airflow user before installing Python libraries
USER airflow

# ✅ Install Python Libraries
RUN pip install --no-cache-dir \
    apache-airflow==2.9.0 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    pyspark==3.5.0 \
    psycopg2-binary \
    minio \
    requests \
    boto3 \
    xxhash

# ✅ Ensure Airflow is in PATH
ENV PATH="/home/airflow/.local/bin:$PATH"

# ✅ Entrypoint
ENTRYPOINT ["/usr/bin/tini", "--"]