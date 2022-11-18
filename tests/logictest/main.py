#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os

from mysql_runner import TestMySQL
from http_runner import TestHttp
from clickhouse_runner import TestClickhouse

from statistics import global_statistics
from argparse import ArgumentParser
from config import mysql_config, http_config, clickhouse_config


TEST_CLASSES = {
    "mysql": TestMySQL,
    "http": TestHttp,
    "clickhouse": TestClickhouse,
}

TEST_CONFIGS = {
    "mysql": mysql_config,
    "http": http_config,
    "clickhouse": clickhouse_config,
}


def run(args):
    """
    Run tests
    """

    for handler in args.handlers.split(","):
        if handler not in TEST_CLASSES:
            raise Exception("Unknown test handler: {}".format(handler))
        test_instance = TEST_CLASSES[handler](handler, args)
        test_instance.set_driver(TEST_CONFIGS[handler])
        test_instance.set_label(handler)
        test_instance.run_sql_suite()

    global_statistics.check_and_exit()


if __name__ == "__main__":
    parser = ArgumentParser(description="databend sqlogical tests")

    parser.add_argument("pattern", nargs="*", help="Optional test case name regex")

    parser.add_argument(
        "--handlers",
        default="mysql,http,clickhouse",
        help="Test handlers, separated by comma",
    )

    parser.add_argument(
        "--test-runs",
        default=1,
        nargs="?",
        type=int,
        help="Run each test many times (useful for e.g. flaky check)",
    )

    parser.add_argument("-q", "--suites", default="suites", help="Path to suites dir")

    parser.add_argument(
        "--skip",
        nargs="+",
        default=os.environ.get("SKIP_TEST_FILES"),
        help="Skip these tests via case name regex",
    )

    parser.add_argument("--skip-dir", nargs="+", help="Skip all these tests in the dir")

    parser.add_argument("--run-dir", nargs="+", help="Only run these tests in the dir")

    args = parser.parse_args()
    run(args)
