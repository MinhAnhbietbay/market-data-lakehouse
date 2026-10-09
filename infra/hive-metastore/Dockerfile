FROM apache/hive:3.1.3

USER root

ARG POSTGRESQL_JDBC_VERSION=42.7.4
ARG HADOOP_AWS_VERSION=3.1.0
ARG AWS_SDK_BUNDLE_VERSION=1.11.271

# Jar JDBC + Hadoop-AWS + AWS SDK được bundle sẵn trong ./jars/ (đã tải từ
# upstream, hash đã verify khi thêm vào repo). COPY thay vì curl trong build
# để Dockerfile không phụ thuộc vào mirror Debian ELTS (bullseye-security pool
# 404 định kỳ khi team ELTS rotate). Xem k8s/Makefile docker-build-hive-metastore.
COPY jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar       /opt/hive/lib/postgresql-${POSTGRESQL_JDBC_VERSION}.jar
COPY jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar            /opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar
COPY jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

COPY jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar            /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar
COPY jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
COPY jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar            /opt/hadoop/share/hadoop/common/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar
COPY jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml
COPY conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

USER hive

EXPOSE 9083

ENTRYPOINT ["/opt/hive/bin/hive"]
CMD ["--service", "metastore"]
