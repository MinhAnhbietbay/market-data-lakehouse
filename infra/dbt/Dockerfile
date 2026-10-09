# Nền là image Spark đã có (Spark 3.5.5 + Iceberg 1.7.1 + conf catalog), nên
# dbt-spark method=session dùng lại đúng cấu hình mà pipelines/raw đang
# chạy -- một nguồn sự thật cho catalog, không phải hai.
#
# ARG SPARK_BASE: registry + repo path của base image. Mặc định
# `alpha-lakehouse/spark` (resolve qua local daemon). CI/local-dev override
# thành `<registry>/alpha-lakehouse/spark` để pull qua registry local —
# vì context của dbt là `infra/dbt/`, BuildKit không nhìn thấy image
# `alpha-lakehouse/spark:local` đã build ở context khác trong cùng daemon.
ARG SPARK_BASE=market-data-lakehouse/spark
FROM ${SPARK_BASE}:local

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends git; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dbt-core==1.9.*" \
    "dbt-spark[PyHive]==1.9.*"

# method=session dựng SparkSession trong tiến trình, nên pyspark phải nằm
# trên PYTHONPATH. Dùng bản đi kèm SPARK_HOME thay vì pip install pyspark --
# hai bản Spark khác nhau trong một image là nguồn lỗi khó truy.
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

WORKDIR /workspace/analytics/dbt

ENTRYPOINT ["dbt"]
