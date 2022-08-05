FROM python:3.10.4-alpine3.15

RUN /usr/local/bin/python -m pip install --upgrade pip && pip install --user mysql-connector six PyHamcrest requests environs fire
RUN pip install https://github.com/youngsofun/clickhouse-sqlalchemy/archive/a116e3162c699c12e63a689385b547f639c13018.zip
COPY *.py /
COPY suites /suites
WORKDIR /

ENTRYPOINT ["python"]
CMD ["main.py"]
