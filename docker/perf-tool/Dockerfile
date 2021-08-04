FROM python:3
COPY ./tests/perfs/perfs.py /perfs.py
COPY ./tests/perfs/compare.py /compare.py
COPY ./distro/fuse-benchmark /fuse-benchmark

COPY ./tests/perfs/perfs.yaml /perfs.yaml

RUN pip install --no-cache-dir pyyaml \
    && pip install -U cos-python-sdk-v5 \
    && python3 -m pip install configargparse\
    && pip install wget
ENV BIN_LOCATION "/fuse-benchmark"
ENV SERVER_HOST "127.0.0.1"
ENV SERVER_PORT "9090"
ENV ITERATION "3"
ENV CONCURRENCY "1"
CMD ["python", "./perfs.py", "-b", "${BIN_LOCATION}", "--host", "${SERVER_HOST}", "-p", "${SERVER_PORT}", "-c", "${CONCURRENCY}}", "-i", "${ITERATION}"]