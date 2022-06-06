import json
import os

import environs
import requests
from mysql.connector.errors import Error
from log import log

headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

default_database = "default"


def format_result(results):
    res = ""
    if results is None:
        return ""

    for line in results:
        lineTmp = ""
        for item in line:
            if isinstance(item, bool):
                item = str.lower(str(item))
            if lineTmp == "":
                lineTmp = str(item)
            else:
                lineTmp = lineTmp + " " + str(
                    item)  # every item seperate by space
        if len(lineTmp) == 0:
            # empty line in results will replace with tab
            lineTmp = "\t"
        res = res + lineTmp + "\n"
    return res


def get_data_type(field):
    if 'data_type' in field:
        if 'inner' in field['data_type']:
            return field['data_type']['inner']['type']
        else:
            return field['data_type']['type']


def get_query_options(response):
    ret = ""
    if get_error(response) != None:
        return ret
    for field in response['schema']['fields']:
        type = str.lower(get_data_type(field))
        log.debug("type:{}".format(type))
        if "int" in type:
            ret = ret + "I"
        elif "float" in type or "double" in type:
            ret = ret + "F"
        elif "bool" in type:
            ret = ret + "B"
        else:
            ret = ret + "T"
    return ret


def get_next_uri(response):
    if "next_uri" in response:
        return response['next_uri']
    return None


def get_result(response):
    return response['data']


def get_error(response):
    # Get mysql-connector Error
    if response['error'] is None:
        return None

    # Wrap errno into msg, for result check
    wrapMsg = "errno:{},msg:{}".format(response['error']['code'],
                                       response['error']['message'])
    return Error(msg=wrapMsg, errno=response['error']['code'])


class HttpConnector():
    # Databend http hander doc: https://databend.rs/doc/reference/api/rest

    # Call connect(**driver)
    # driver is a dict contains:
    # {
    #   'user': 'root',
    #   'host': '127.0.0.1',
    #   'port': 3307,
    #   'database': 'default'
    # }
    def connect(self, host, port, user="root", database=default_database):
        self._host = host
        self._port = port
        self._user = user
        self._database = database
        self._session_max_idle_time = 300
        self._session = None
        self._additonal_headers = dict()
        e = environs.Env()
        if os.getenv("ADDITIONAL_HEADERS") is not None:
            self._additonal_headers = e.dict("ADDITIONAL_HEADERS")

    def query(self, statement, session=None):
        url = "http://{}:{}/v1/query/".format(self._host, self._port)

        def parseSQL(sql):
            # for cases like:
            # select "false"::boolean = not "true"::boolean;  => select 'false'::boolean = not 'true'::boolean;
            # SELECT parse_json('"false"')::boolean;          => SELECT parse_json('\"false\"')::boolean;
            if '"' in sql:
                if '\'' in sql:
                    return str.replace(sql, '"', '\\\"')  #  "  -> \"
                return str.replace(sql, "\"", "'")  #  "  -> '
            else:
                return sql  #  do nothing

        log.debug("http sql: " + parseSQL(statement))
        query_sql = {'sql': parseSQL(statement)}
        if session is not None:
            query_sql['session'] = session
        log.debug("http headers: {}".format({
            **headers,
            **self._additonal_headers
        }))
        if "Authorization" not in self._additonal_headers:
            response = requests.post(url,
                                     data=json.dumps(query_sql),
                                     auth=(self._user, ""),
                                     headers=headers)
        else:
            response = requests.post(url,
                                     data=json.dumps(query_sql),
                                     headers={
                                         **headers,
                                         **self._additonal_headers
                                     })

        try:
            return json.loads(response.content)
        except Exception as err:
            log.error("http error, SQL: {}".format(statement))
            log.error("content: {}".format(response.content))
            raise

    def set_database(self, database):
        self._database = database

    def query_without_session(self, statement):
        return self.query(statement, {"database": self._database})

    def reset_session(self):
        self._database = default_database
        self._session = None

    # query_with_session keep session_id for every query
    def query_with_session(self, statement):
        current_session = self._session
        if current_session is None:
            # new session
            current_session = {
                "database": self._database,
                "max_idle_time": self._session_max_idle_time
            }

        response = self.query(statement, current_session)
        if self._session is None:
            if response is not None and "session_id" in response:
                self._session = {"id": response["session_id"]}
        return response

    def fetch_all(self, statement):
        # TODO use next_uri to get all results
        resp = self.query_with_session(statement)
        if resp is None:
            log.warning("fetch all with empty results")
            return None
        self._query_option = get_query_options(resp)  # record schema
        return get_result(resp)

    def get_query_option(self):
        return self._query_option


# if __name__ == '__main__':
#     from config import http_config
#     connector = HttpConnector()
#     connector.connect(**http_config)
#     connector.query_without_session("show databases;")
