FROM python:3.10.4-alpine3.15

RUN /usr/local/bin/python -m pip install --upgrade pip && pip install --user mysql-connector six PyHamcrest requests environs fire
RUN pip install https://github.com/youngsofun/clickhouse-sqlalchemy/archive/dfd9ba3540dcb3c88f0850ef30746738bc6915f7.zip
COPY *.py /
COPY suites/* /suites/
WORKDIR /

ENTRYPOINT ["python"]
CMD ["main.py"]
