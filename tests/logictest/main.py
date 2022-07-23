#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os

from mysql_runner import TestMySQL
from http_runner import TestHttp
from clickhouse_runner import TestClickhouse

from argparse import ArgumentParser
from config import mysql_config, http_config, clickhouse_config


def run(args):
    """
    Run tests
    """
    if not args.disable_mysql_test:
        mysql_test = TestMySQL("mysql", args.pattern)
        mysql_test.set_driver(mysql_config)
        mysql_test.set_label("mysql")
        mysql_test.run_sql_suite()

    if not args.disable_http_test:
        http = TestHttp("http", args.pattern)
        http.set_driver(http_config)
        http.set_label("http")
        http.run_sql_suite()

    if not args.disable_clickhouse_test:
        http = TestClickhouse("clickhouse", args.pattern)
        http.set_driver(clickhouse_config)
        http.set_label("clickhouse")
        http.run_sql_suite()


if __name__ == '__main__':
    parser = ArgumentParser(description='databend sqlogical tests')
    parser.add_argument('--disable-mysql-test',
                        action='store_true',
                        default=os.environ.get('DISABLE_MYSQL_LOGIC_TEST'),
                        help='Disable mysql handler test')
    parser.add_argument('--disable-http-test',
                        action='store_true',
                        default=os.environ.get('DISABLE_HTTP_LOGIC_TEST'),
                        help='Disable http handler test')
    parser.add_argument('--disable-clickhouse-test',
                        action='store_true',
                        default=os.environ.get('DISABLE_CLICKHOUSE_LOGIC_TEST'),
                        help='Disable clickhouse handler test')
    parser.add_argument('pattern',
                        nargs='*',
                        help='Optional test case name regex')

    args = parser.parse_args()
    run(args)
