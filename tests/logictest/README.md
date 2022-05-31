
# Targets

The database return right with different handlers, for example mysql and http

# Usage

## prepare
1. Change to the scripts dir, cd tests/logictest/
2. Make sure python3 is installed
3. Using [Poetry](https://github.com/python-poetry/poetry) to install dependency, dependency see tests/pyproject.toml
4. (Optional)If use pip, you can find dependency in Dockerfile

## Need to known
1. Cases from **tests/suites/0_stateless/**  to  **tests/logictest/suites/gen/**
2. If a case file already exists in gen/, gen_suites will ignore it. 
3. Regenerate：delete case file in gen/ and run gen_suites.py

## Generate sqllogic test cases from Stateless Test
1. python3 gen_suites.py

## Usage
You can simply run all tests with:
```shell
python main.py
```

Get help with:
```shell
python main.py -h
```

## Docker

### Build image

docker build -t sqllogic/test:latest .

### Run with docker

1. Image release: public.ecr.aws/k3y0u5f2/sqllogic/test:latest
2. Set envs
- SKIP_TEST_FILES (skip test case, set file name here split by `,` )
- QUERY_MYSQL_HANDLER_HOST
- QUERY_MYSQL_HANDLER_PORT
- QUERY_HTTP_HANDLER_HOST
- QUERY_HTTP_HANDLER_PORT
- MYSQL_DATABASE
- MYSQL_USER
- ADDITIONAL_HEADERS (for security scenario)
3. docker run --name logictest --rm --network host public.ecr.aws/k3y0u5f2/sqllogic/test:latest

# Learn More

Ref pr: https://github.com/datafuselabs/databend/pull/5048
