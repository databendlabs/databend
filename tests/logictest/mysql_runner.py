from abc import ABC

import mysql.connector

import logictest
from log import log


class TestMySQL(logictest.SuiteRunner, ABC):

    def __init__(self, kind):
        super().__init__(kind)
        self._connection = None

    def get_connection(self):
        if self._connection is not None:
            return self._connection
        self._connection = mysql.connector.connect(**self.driver)
        return self._connection

    def execute_ok(self, statement):
        cursor = self.get_connection().cursor(buffered=True)
        cursor.execute(statement)
        return None

    def execute_error(self, statement):
        cnx = mysql.connector.connect(**self.driver)
        cursor = cnx.cursor()
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
                    if not isinstance(v, str):
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
