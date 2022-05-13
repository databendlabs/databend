from abc import ABC

import logictest
import http_connector
from log import log

class TestHttp(logictest.SuiteRunner, ABC):

    def execute_ok(self, statement):
        http = http_connector.HttpConnector()
        http.connect(**self.driver)
        http.query_without_session(statement)
        return None

    def execute_error(self, statement):
        http = http_connector.HttpConnector()
        http.connect(**self.driver)
        resp = http.query_without_session(statement)
        return http_connector.get_error(resp)

    def execute_query(self, statement):
        http = http_connector.HttpConnector()
        http.connect(**self.driver)
        results = http.fetch_all(statement.text)
        query_type = statement.s_type.query_type
        vals = []
        for (ri,row) in enumerate(results):
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
                    if not isinstance(v, bool):
                        log.error(
                            "Expected bool, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                    v= str(v).lower() # bool to string in python will be True/False
                else:
                    log.error(
                        "Unknown type {} in query {} row {} col {} value {}".
                        format(query_type[i], statement.text, ri, i, v))
                vals.append(str(v))
        return vals
