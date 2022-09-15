from abc import ABC
from datetime import datetime, date

import mysql.connector

import logictest
from log import log

import mysql.connector
from mysql.connector.conversion import MySQLConverter


class StringConverter(MySQLConverter):

    def _datetime_to_python(self, value, dsc=None):
        if not value:
            return None
        return MySQLConverter._string_to_python(self, value)


class TestMySQL(logictest.SuiteRunner, ABC):

    def __init__(self, kind, args):
        super().__init__(kind, args)
        self._connection = None

    def reset_connection(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def get_connection(self):
        if self._connection is not None:
            return self._connection
        if "converter_class" not in self.driver:
            self.driver["converter_class"] = StringConverter
        self._connection = mysql.connector.connect(**self.driver)
        return self._connection

    def batch_execute(self, statement_list):
        for statement in statement_list:
            self.execute_statement(statement)
        self.reset_connection()

    def execute_ok(self, statement):
        cursor = self.get_connection().cursor(buffered=True)
        cursor.execute(statement)
        return None

    def execute_error(self, statement):
        cursor = cursor = self.get_connection().cursor(buffered=True)
        try:
            cursor.execute(statement)
        except mysql.connector.Error as err:
            return err
        return None

    def execute_query(self, statement):
        cursor = self.get_connection().cursor(buffered=True)
        cursor.execute(statement.text)
        r = cursor.fetchall()
        query_type = statement.s_type.query_type
        vals = []
        for (ri, row) in enumerate(r):
            for (i, v) in enumerate(row):
                if isinstance(v, type(None)):
                    vals.append("NULL")
                    continue

                if query_type[i] == 'I':
                    if not isinstance(v, int):
                        log.debug(
                            f"Expected int, got type {type(v)} in query {statement.text} row {ri} col {i} value {v}"
                        )
                elif query_type[i] == 'F' or query_type[i] == 'R':
                    if not isinstance(v, float):
                        log.debug(
                            f"Expected float, got type {type(v)} in query {statement.text} row {ri} col {i} value {v}"
                        )
                elif query_type[i] == 'T':
                    if not (isinstance(v, str) or isinstance(v, datetime) or
                            isinstance(v, date)):
                        log.debug(
                            f"Expected string, got type {type(v)} in query { statement.text} row {ri} col {i} value {v}"
                        )
                elif query_type[i] == 'B':
                    # bool return int in mysql
                    if not isinstance(v, int):
                        log.debug(
                            f"Expected Bool, got type {type(v)} in query {statement.text} row {ri} col {i} value {v}"
                        )
                else:
                    log.debug(
                        f"Unknown type {query_type[i]} in query {statement.text} row {ri} col {i} value {v}"
                    )
                vals.append(str(v))
        return vals
