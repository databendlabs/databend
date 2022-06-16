FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y git build-essential

# Use https://github.com/databricks/tpch-dbgen to generate data
RUN git clone https://github.com/databricks/tpch-dbgen.git && cd tpch-dbgen && make

WORKDIR /tpch-dbgen
ADD run-tpch-dbgen.sh /tpch-dbgen/

VOLUME /data

ENTRYPOINT [ "bash", "./run-tpch-dbgen.sh" ]
