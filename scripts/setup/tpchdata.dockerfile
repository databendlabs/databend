FROM ubuntu:22.04
ARG scale_factor=1
ENV scale_factor=$scale_factor
RUN apt-get update && \
    apt-get install -y git build-essential

# Use https://github.com/databricks/tpch-dbgen to generate data
RUN git clone https://github.com/databricks/tpch-dbgen.git && cd tpch-dbgen && make

WORKDIR /tpch-dbgen
ADD scripts/setup/run-tpch-dbgen.sh /tpch-dbgen/

VOLUME /data

SHELL ["/bin/bash", "-c"]
RUN chmod +x run-tpch-dbgen.sh
ENTRYPOINT ./run-tpch-dbgen.sh $scale_factor
