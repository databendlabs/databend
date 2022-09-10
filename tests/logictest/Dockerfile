FROM python:3.10

RUN python3 -m pip install --upgrade pip && pip install --user mysql-connector six PyHamcrest requests environs fire
RUN pip install https://github.com/youngsofun/clickhouse-sqlalchemy/archive/a116e3162c699c12e63a689385b547f639c13018.zip
RUN mkdir /test
WORKDIR /test
COPY *.py /test

VOLUME ["/test/suites"]

CMD ["/test/main.py"]
