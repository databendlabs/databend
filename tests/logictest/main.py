#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from mysql_runner import TestMySQL
from http_runner import TestHttp

from config import mysql_config,http_config

if __name__ == '__main__':
    mySQL = TestMySQL("mysql")
    mySQL.set_driver(mysql_config)
    mySQL.set_label("mysql")
    mySQL.run_sql_suite()

    http = TestHttp("http")
    http.set_driver(http_config)
    http.set_label("http")
    http.run_sql_suite()

