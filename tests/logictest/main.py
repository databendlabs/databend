#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os

from mysql_runner import TestMySQL
from http_runner import TestHttp
from clickhouse_runner import TestClickhouse

from statistics import global_statistics
from argparse import ArgumentParser
from config import mysql_config, http_config, clickhouse_config
from cleanup import set_auto_cleanup


def run(args):
    """
    Run tests
    """
    set_auto_cleanup(args.enable_auto_cleanup)

    if not args.disable_mysql_test:
        mysql_test = TestMySQL("mysql", args)
        mysql_test.set_driver(mysql_config)
        mysql_test.set_label("mysql")
        mysql_test.run_sql_suite()

    if not args.disable_http_test:
        http = TestHttp("http", args)
        http.set_driver(http_config)
        http.set_label("http")
        http.run_sql_suite()

    if not args.disable_clickhouse_test:
        http = TestClickhouse("clickhouse", args)
        http.set_driver(clickhouse_config)
        http.set_label("clickhouse")
        http.run_sql_suite()

    global_statistics.check_and_exit()


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
    parser.add_argument('--enable-auto-cleanup',
                        action='store_true',
                        default=False,
                        help="Enable auto cleanup after test per session")
    parser.add_argument('pattern',
                        nargs='*',
                        help='Optional test case name regex')

    parser.add_argument(
        '--test-runs',
        default=1,
        nargs='?',
        type=int,
        help='Run each test many times (useful for e.g. flaky check)')

    parser.add_argument('-q',
                        '--suites',
                        default="suites",
                        help='Path to suites dir')

    parser.add_argument('--skip',
                        nargs='+',
                        default=os.environ.get('SKIP_TEST_FILES'),
                        help="Skip these tests via case name regex")

    parser.add_argument('--skip-dir',
                        nargs='+',
                        help="Skip all these tests in the dir")

    parser.add_argument('--run-dir',
                        nargs='+',
                        help="Only run these tests in the dir")

    args = parser.parse_args()
    run(args)
