from abc import ABC
from types import NoneType

import logictest
import http_connector
from log import log


class TestHttp(logictest.SuiteRunner, ABC):

    def __init__(self, kind, pattern):
        super().__init__(kind, pattern)
        self._http = None

    def get_connection(self):
        if self._http is None:
            self._http = http_connector.HttpConnector()
            self._http.connect(**self.driver)
        return self._http

    def reset_connection(self):
        self._http.reset_session()

    def batch_execute(self, statement_list):
        for statement in statement_list:
            self.execute_statement(statement)
        self.reset_connection()

    def execute_ok(self, statement):
        self.get_connection().query_with_session(statement)
        return None

    def execute_error(self, statement):
        resp = self.get_connection().query_with_session(statement)
        return http_connector.get_error(resp)

    def execute_query(self, statement):
        results = self.get_connection().fetch_all(statement.text)
        query_type = statement.s_type.query_type
        vals = []
        for (ri, row) in enumerate(results):
            for (i, v) in enumerate(row):
                if isinstance(v, NoneType):
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
                    # include data, timestamp, dict, list ...
                    if not (isinstance(v, str) or isinstance(v, dict) or
                            isinstance(v, list)):
                        log.error(
                            "Expected string, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                elif query_type[i] == 'B':
                    if not isinstance(v, bool):
                        log.error(
                            "Expected bool, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                else:
                    log.error(
                        "Unknown type {} in query {} row {} col {} value {}".
                        format(query_type[i], statement.text, ri, i, v))
                if isinstance(v, bool):
                    v = str(v).lower(
                    )  # bool to string in python will be True/False
                vals.append(str(v))
        return vals
