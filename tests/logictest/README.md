
# Targets

The database return right with diffrent handlers, for example mysql and http

# Usage

## prepare
1. Change to the scripts dir, cd tests/logictest/
2. Make sure python3 is installed
3. Using pip to install dependency, pip install -r ./requirements.txt  (use -i for private repo)

## Need to Known
1. Cases from **tests/suites/0_stateless/**  to  **tests/logictest/suites/gen/**
2. If a case file already exists in gen/, gen_suites will ignore it. 
3. Regenerate：delete case file in gen/ and run gen_suites.py

## Generate sqllogic test cases from Stateless Test
1. python3 gen_suites.py

## Run logic test
1. python3 main.py

# Learn More

See pr: https://github.com/datafuselabs/databend/pull/5048
