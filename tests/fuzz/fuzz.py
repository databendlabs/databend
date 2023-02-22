#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import re
import os
import random
import time
import mysql.connector
from fuzzingbook.Grammars import Grammar, is_valid_grammar


class mysql_client:
    def __init__(self):
        config = {
            "user": "root",
            "host": "127.0.0.1",
            "port": 3307,
            "database": "default",
        }
        host = os.getenv("QUERY_MYSQL_HANDLER_HOST")
        if host is not None:
            config["host"] = host
        port = os.getenv("QUERY_MYSQL_HANDLER_PORT")
        if port is not None:
            config["port"] = port
        user = os.getenv("MYSQL_USER")
        if user is not None:
            config["user"] = user
        default_database = os.getenv("MYSQL_DATABASE")
        if default_database is not None:
            config["database"] = default_database
        self._connection = mysql.connector.connect(**config)

    def close(self):
        self._connection.close()

    def run(self, sql):
        cursor = self._connection.cursor(buffered=True)
        try:
            cursor.execute(sql)
            return None
        except Exception as err:
            print("sql: {} execute with error: {} ".format(sql, str(err)))
            return err


START_SYMBOL = "<start>"
RE_NONTERMINAL = re.compile(r"(<[^<> ]*>)")


def nonterminals(expansion):
    if isinstance(expansion, tuple):
        expansion = expansion[0]

    return RE_NONTERMINAL.findall(expansion)


prepare_sqls = [
    "drop table if exists t1;",
    "drop table if exists t2;",
    "drop table if exists t3;",
    "CREATE TABLE IF NOT EXISTS random_t1 (row1 INT, row2 INT NULL, row3 FLOAT, row4 BOOLEAN, row5 VARCHAR, row6 DATE, row7 TIMESTAMP, row8 ARRAY(INT)) ENGINE=RANDOM;",
    "CREATE TABLE IF NOT EXISTS random_t2 (row1 INT, row2 INT NULL, row3 FLOAT, row4 BOOLEAN, row5 VARCHAR, row6 DATE, row7 TIMESTAMP, row8 ARRAY(INT)) ENGINE=RANDOM;",
    "CREATE TABLE IF NOT EXISTS random_t3 (row1 INT, row2 INT NULL, row3 FLOAT, row4 BOOLEAN, row5 VARCHAR, row6 DATE, row7 TIMESTAMP, row8 ARRAY(INT)) ENGINE=RANDOM;",
    "CREATE TABLE IF NOT EXISTS t1 (row1 INT, row2 INT NULL, row3 FLOAT, row4 BOOLEAN, row5 VARCHAR, row6 DATE, row7 TIMESTAMP, row8 ARRAY(INT));",
    "CREATE TABLE IF NOT EXISTS t2 (row1 INT, row2 INT NULL, row3 FLOAT, row4 BOOLEAN, row5 VARCHAR, row6 DATE, row7 TIMESTAMP, row8 ARRAY(INT));",
    "CREATE TABLE IF NOT EXISTS t3 (row1 INT, row2 INT NULL, row3 FLOAT, row4 BOOLEAN, row5 VARCHAR, row6 DATE, row7 TIMESTAMP, row8 ARRAY(INT));",
    "insert into t1 select * from random_t1 limit 110;",
    "insert into t2 select * from random_t2 limit 110;",
    "insert into t3 select * from random_t3 limit 110;",
]

# Select grammar RFC: https://github.com/datafuselabs/databend/issues/4916
# Here is a minimal implement
select_grammar: Grammar = {
    "<start>": [
        "SELECT <select_list> FROM <table_reference_list> <limit_list>",
        "SELECT <select_list> FROM <table_reference_list> where <condition_expression> <limit_list>",
        "SELECT <function_reference>(<target_rows>) FROM <table_reference_list> where <condition_expression> group by <group_by_list> <limit_list>",
        "SELECT <function_reference>(<target_rows>) FROM <table_reference_list> group by <group_by_list> <limit_list>",
        "SELECT <select_list> FROM <table_reference_list> order by <expression> ASC <limit_list>",
        "SELECT <select_list> FROM <table_reference_list> order by <expression> DESC <limit_list>",
    ],
    "<select_list>": ["*", "<select_target>", "<select_target>,<select_target>"],
    "<select_target>": ["<target_rows>"],
    "<condition_expression>": ["<target_rows> <expr> <value>"],
    "<table_reference_list>": [
        "<table_reference>",
        "<table_reference>, <table_reference>",
    ],
    "<function_reference>": ["sum", "avg", "count", "min", "max"],
    "<group_by_list>": ["<expression>", "<following_expression>"],
    "<limit_list>": ["limit 1", "limit 10", "limit 100"],
    "<following_expression>": ["<expression>", "<expression>,<expression>"],
    "<expression>": ["<target_rows>", "<target_rows>,<target_rows>"],
    "<table_reference>": ["t1", "t2", "t3"],
    "<target_rows>": ["row1", "row2", "row3", "row4", "row5", "row6", "row7", "row8"],
    "<expr>": [">", "<", ">=", "<=", "!=", "="],
    "<value>": ["1", "0", "null"],
}

drop_grammar: Grammar = {
    "<start>": ["drop table <drop_option> <table_reference> <all_reference>"],
    "<drop_option>": ["if exists", ""],
    "<table_reference>": ["t1", "t2", "t3"],
    "<all_reference>": ["all", ""],
}

assert is_valid_grammar(select_grammar)
assert is_valid_grammar(drop_grammar)


class ExpansionError(Exception):
    pass


# https://www.fuzzingbook.org/html/Grammars.html
def grammar_fuzzer(
    grammar,
    start_symbol: str = START_SYMBOL,
    max_nonterminals: int = 10,
    max_expansion_trials: int = 100,
    log: bool = False,
) -> str:
    term = start_symbol
    expansion_trials = 0

    while len(nonterminals(term)) > 0:
        symbol_to_expand = random.choice(nonterminals(term))
        expansions = grammar[symbol_to_expand]
        expansion = random.choice(expansions)
        if isinstance(expansion, tuple):
            expansion = expansion[0]

        new_term = term.replace(symbol_to_expand, expansion, 1)

        if len(nonterminals(new_term)) < max_nonterminals:
            term = new_term
            if log:
                print("%-40s" % (symbol_to_expand + " -> " + expansion), term)
            expansion_trials = 0
        else:
            expansion_trials += 1
            if expansion_trials >= max_expansion_trials:
                raise ExpansionError("Cannot expand " + repr(term))
    return term


class QueryGenerator:
    def __init__(self, grammar, execute_times=100):
        random.seed(time.time())
        self._grammar = grammar
        self.execute_times = execute_times
        self._max_nonterminals = random.randint(5, 10)

    def next(self):
        return grammar_fuzzer(self._grammar, max_nonterminals=self._max_nonterminals)


class QueryExecutor:
    def __init__(self):
        self._client = mysql_client()
        self.prepare()

    # create random tables
    def prepare(self):
        for sql in prepare_sqls:
            self._client.run(sql)

    def execute(self, sql):
        return self._client.run(sql)

    def close(self):
        self._client.close()
        self._client = None


class FuzzRunner:
    def __init__(self, generators, executor):
        self._generators = generators
        self._executor = executor

    def run(self):
        for generator in self._generators:
            for i in range(generator.execute_times):
                query = generator.next()
                if not query_validate(self._executor.execute(query)):
                    raise ExpansionError("query {} failed".format(query))
        self._executor.close()


def query_validate(result):
    if result is None:  # sql success
        return True
    if "Code" in str(result):  # sql error
        return True
    return False  # other error as a failed test


generator_list = [
    QueryGenerator(select_grammar, 1000),
    QueryGenerator(drop_grammar, 10),
]

if __name__ == "__main__":
    f = FuzzRunner(generator_list, QueryExecutor())
    f.run()
