# Fuzz test

Fuzz test get sql in given grammar, execute it using mysql client.

## failed condition

Result is not a mysql error or None is a failed test.

## python dependency

1. python3
2. pip3 install fuzzingbook mysql-connector

## add new grammar fuzzer

1. Add new grammar, as a example: select_grammar:Grammar = {}
2. Add validate of new grammar, assert is_valid_grammar(select_grammar)
3. Add grammar to generator list with fuzz times, generator_list = [...]
4. Run fuzz.py