FROM python:3.10

WORKDIR /test

RUN mkdir /test
COPY *.py /test
COPY requirements.txt /test

RUN apt-get update -yq && apt-get install -yq \
    psmisc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip && pip install -r requirements.txt

VOLUME ["/test/suites"]
CMD ["/test/main.py"]
