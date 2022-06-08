from abc import ABC
from datetime import datetime, date

import mysql.connector

import logictest
from log import log


class TestMySQL(logictest.SuiteRunner, ABC):

    def __init__(self, kind, pattern):
        super().__init__(kind, pattern)
        self._connection = None

    def reset_connection(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def get_connection(self):
        if self._connection is not None:
            return self._connection
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
                        log.error(
                            "Expected int, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                elif query_type[i] == 'F' or query_type[i] == 'R':
                    if not isinstance(v, float):
                        log.error(
                            "Expected float, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                elif query_type[i] == 'T':
                    if not (isinstance(v, str) or isinstance(v, datetime) or
                            isinstance(v, date)):
                        log.error(
                            "Expected string, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                elif query_type[i] == 'B':
                    # bool return int in mysql
                    if not isinstance(v, int):
                        log.error(
                            "Expected Bool, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                else:
                    log.error(
                        "Unknown type {} in query {} row {} col {} value {}".
                        format(query_type[i], statement.text, ri, i, v))
                vals.append(str(v))
        return vals
