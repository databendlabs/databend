#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os

from mysql_runner import TestMySQL
from http_runner import TestHttp

from config import mysql_config, http_config

if __name__ == '__main__':
    disable_mysql_test = os.getenv("DISABLE_MYSQL_LOGIC_TEST")
    if disable_mysql_test is not None:
        mySQL = TestMySQL("mysql")
        mySQL.set_driver(mysql_config)
        mySQL.set_label("mysql")
        mySQL.run_sql_suite()

    disable_http_test = os.getenv("DISABLE_HTTP_LOGIC_TEST")
    if disable_http_test is not None:
        http = TestHttp("http")
        http.set_driver(http_config)
        http.set_label("http")
        http.run_sql_suite()
