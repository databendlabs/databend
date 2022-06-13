#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os

from mysql_runner import TestMySQL
from http_runner import TestHttp

from config import mysql_config, http_config

import fire


def run(pattern=".*"):
    """
    Run tests
    """
    disable_mysql_test = os.getenv("DISABLE_MYSQL_LOGIC_TEST")
    if disable_mysql_test is None:
        mysql_test = TestMySQL("mysql", pattern)
        mysql_test.set_driver(mysql_config)
        mysql_test.set_label("mysql")
        mysql_test.run_sql_suite()

    disable_http_test = os.getenv("DISABLE_HTTP_LOGIC_TEST")
    if disable_http_test is None:
        http = TestHttp("http", pattern)
        http.set_driver(http_config)
        http.set_label("http")
        http.run_sql_suite()


if __name__ == '__main__':
    fire.Fire(run)
