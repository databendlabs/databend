from abc import ABC
from log import log
from mysql.connector.errors import Error

import logictest
import clickhouse_connector


class TestClickhouse(logictest.SuiteRunner, ABC):

    def __init__(self, kind, pattern):
        super().__init__(kind, pattern)
        self._ch = None

    def get_connection(self):
        if self._ch is None:
            self._ch = clickhouse_connector.ClickhouseConnector()
            self._ch.connect(**self.driver)
        return self._ch

    def reset_connection(self):
        self._ch.reset_session()

    def batch_execute(self, statement_list):
        for statement in statement_list:
            self.execute_statement(statement)
        self.reset_connection()

    def execute_ok(self, statement):
        self.get_connection().query_with_session(statement)
        return None

    def execute_error(self, statement):
        try:
            self.get_connection().query_with_session(statement)
        except Exception as err:
            return Error(msg=str(err))

    def execute_query(self, statement):
        results = self.get_connection().fetch_all(statement.text)
        log.debug(results)
        # query_type = statement.s_type.query_type
        vals = []
        for (ri, row) in enumerate(results):
            for (i, v) in enumerate(row):
                if isinstance(v, type(None)):
                    vals.append("NULL")
                    continue

                vals.append(str(v))
        return vals
