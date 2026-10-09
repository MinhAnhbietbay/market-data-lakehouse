FROM apache/spark:3.5.8-python3 AS spark-dist

FROM python:3.11-slim-bookworm

ARG ICEBERG_VERSION=1.9.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        bash \
        curl \
        openjdk-17-jre-headless \
        procps \
        tini; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    pip install --no-cache-dir --timeout 300 --retries 5 \
        --trusted-host pypi.org --trusted-host files.pythonhosted.org \
        'PyYAML>=6.0.1'

COPY --from=spark-dist /opt/spark ${SPARK_HOME}

RUN set -eux; \
    curl --retry 5 --retry-delay 2 -fL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl --retry 5 --retry-delay 2 -fL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl --retry 5 --retry-delay 2 -fL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

WORKDIR /workspace

COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
