from abc import ABC
import logging

import logictest
import http_connector

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

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
                    if not isinstance(v, bytes):
                        log.error(
                            "Expected bytes, got type {} in query {} row {} col {} value {}"
                            .format(type(v), statement.text, ri, i, v))
                else:
                    log.error(
                        "Unknown type {} in query {} row {} col {} value {}".
                        format(query_type[i], statement.text, ri, i, v))
                vals.append(str(v))
        return vals


