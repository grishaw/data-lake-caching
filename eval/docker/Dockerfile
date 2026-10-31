# Local image. Java 8 matches the EMR 6.10 default runtime.
FROM maven:3.9.9-eclipse-temurin-8

ARG HADOOP_AWS_VERSION=3.3.2
ARG AWS_SDK_VERSION=1.11.1026
ARG TPCH_DBGEN_COMMIT=32f1c1b92d1664dba542e927d23d86ffa57aa253

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git curl procps python3 python3-pip python3-venv time \
    && pip3 install --break-system-packages --no-cache-dir pyarrow==15.0.2 numpy==1.26.4 \
    && rm -rf /var/lib/apt/lists/*

# Spark 3.3.1 from the build context. scripts/fetch_spark.sh verified it against Apache's published SHA-512.
COPY spark-3.3.1-bin-hadoop3.tgz /tmp/spark.tgz
RUN mkdir -p /opt/spark && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && rm -f /tmp/spark.tgz

# TPC-H dbgen. This is a community mirror, not the TPC download; Q6 at SF1 is checked against the published answer.
RUN git clone https://github.com/electrum/tpch-dbgen.git /opt/tpch-dbgen \
    && cd /opt/tpch-dbgen && git checkout ${TPCH_DBGEN_COMMIT} \
    && make -j4 CC=gcc DATABASE=ORACLE MACHINE=LINUX WORKLOAD=TPCH CFLAGS="-g -DDBNAME=\\\"dss\\\" -DLINUX -DORACLE -DTPCH -DRNG_TEST -D_FILE_OFFSET_BITS=64 -fcommon -std=gnu89 -w -static" dbgen

# The original pom needs a JDK 11 compiler: tinspin 2.1.3 ships Java 11 class files. Output stays Java 8 bytecode.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jdk-headless && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:$PATH
WORKDIR /work/eval
