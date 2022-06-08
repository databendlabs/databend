From python:3.10.4-alpine3.15

RUN /usr/local/bin/python -m pip install --upgrade pip && pip install --user mysql-connector six PyHamcrest requests environs fire
COPY *.py /
COPY suites/* /suites/
WORKDIR /

ENTRYPOINT ["python"] 
CMD ["main.py"]
