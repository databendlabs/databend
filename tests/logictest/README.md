
# Targets

The database return right with diffrent handlers, for example mysql and http

# Usage

## prepare
1. Change to the scripts dir, cd tests/logictest/
2. Make sure python3 is installed
3. Using [Poetry](https://github.com/python-poetry/poetry) to install dependency, dependency see tests/pyproject.toml

## Need to Known
1. Cases from **tests/suites/0_stateless/**  to  **tests/logictest/suites/gen/**
2. If a case file already exists in gen/, gen_suites will ignore it. 
3. Regenerate：delete case file in gen/ and run gen_suites.py

## Generate sqllogic test cases from Stateless Test
1. python3 gen_suites.py

## Run logic test
1. python3 main.py

## Docker build, or use our latest build.

### build

docker build -t sqllogic/test:latest .

### Usage

1. Image release: public.ecr.aws/k3y0u5f2/sqllogic/test:latest
2. Set envs
- DISABLE_MYSQL_LOGIC_TEST
- DISABLE_HTTP_LOGIC_TEST
- QUERY_MYSQL_HANDLER_HOST
- QUERY_MYSQL_HANDLER_PORT
- MYSQL_DATABASE
- MYSQL_USER
- QUERY_HTTP_HANDLER_HOST
- QUERY_HTTP_HANDLER_PORT
- ADDITIONAL_HEADERS
3. docker run --name logictest --rm --network host public.ecr.aws/k3y0u5f2/sqllogic/test:latest

# Learn More

See pr: https://github.com/datafuselabs/databend/pull/5048
