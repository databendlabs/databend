FROM python:3.10

RUN apt-get update -yq && apt-get install -yq \
    psmisc libffi-dev && \
    rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --upgrade pip && pip install \
    mysql-connector-python==8.0.28 \
    pymysql \
    sqlalchemy \
    clickhouse_driver \
    mysql-connector \
    six \
    PyHamcrest \
    requests \
    environs \
    fire \
    fuzzingbook
RUN pip install https://github.com/youngsofun/clickhouse-sqlalchemy/archive/a116e3162c699c12e63a689385b547f639c13018.zip
RUN mkdir /test
WORKDIR /test
COPY *.py /test

VOLUME ["/test/suites"]

CMD ["/test/main.py"]
