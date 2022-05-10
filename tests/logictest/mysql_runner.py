import logging
from abc import ABC

import mysql.connector

import logictest

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)


class TestMySQL(logictest.SuiteRunner, ABC):

    def execute_ok(self, statement):
        cnx = mysql.connector.connect(**self.driver)
        cursor = cnx.cursor()
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
        cnx = mysql.connector.connect(**self.driver)
        cursor = cnx.cursor()
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
