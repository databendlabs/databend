FROM python:3.12-slim-bookworm

ARG TARGETPLATFORM
ENV TERM=dumb
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y apt-transport-https ca-certificates gdb curl default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV HADOOP_HOME=/opt/hadoop
ENV LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${HADOOP_HOME}/lib/native
ENV CLASSPATH=${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/yarn:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*
COPY ./distro/$TARGETPLATFORM/databend-query /databend-query
RUN useradd --uid 1000 --shell /sbin/nologin \
    --home-dir /var/lib/databend --user-group \
    --comment "Databend cloud data analytics" databend && \
    mkdir -p /var/lib/databend && \
    chown -R databend:databend /var/lib/databend
VOLUME ["/var/lib/databend", "/opt/hadoop"]
ENTRYPOINT ["/databend-query"]
