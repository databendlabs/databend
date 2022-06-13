import json
import os
import time
import base64

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
        self._session_max_idle_time = 30
        self._session = None
        self._additonal_headers = dict()
        e = environs.Env()
        if os.getenv("ADDITIONAL_HEADERS") is not None:
            self._additonal_headers = e.dict("ADDITIONAL_HEADERS")

    def make_headers(self):
        if "Authorization" not in self._additonal_headers:
            return {
                **headers, "Authorization":
                    "Basic " + base64.b64encode("{}:{}".format(
                        self._user, "").encode(encoding="utf-8")).decode()
            }
        else:
            return {**headers, **self._additonal_headers}

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
        query_sql = {'sql': parseSQL(statement), "string_fields": True}
        if session is not None:
            query_sql['session'] = session
        log.debug("http headers {}".format(self.make_headers()))
        response = requests.post(url,
                                 data=json.dumps(query_sql),
                                 headers=self.make_headers())

        try:
            return json.loads(response.content)
        except Exception as err:
            log.error("http error, SQL: {}\ncontent: {}\nerror msg:{}".format(
                statement, response.content, str(err)))
            raise

    def set_database(self, database):
        self._database = database

    def query_without_session(self, statement):
        return self.query(statement, {"database": self._database})

    def reset_session(self):
        self._database = default_database
        self._session = None

    # query_with_session keep session_id for every query
    # return a list of response util empty next_uri
    def query_with_session(self, statement):
        current_session = self._session
        if current_session is None:
            # new session
            current_session = {
                "database": self._database,
                "max_idle_time": self._session_max_idle_time
            }

        response_list = list()
        response = self.query(statement, current_session)
        log.info("response content: {}".format(response))
        response_list.append(response)
        for i in range(12):
            if response['next_uri'] is not None:
                try:
                    resp = requests.get(url="http://{}:{}{}".format(
                        self._host, self._port, response['next_uri']),
                                        headers=self.make_headers())
                    response = json.loads(resp.content)
                    log.info(
                        "Sql in progress, fetch next_uri content: {}".format(
                            response))
                    response_list.append(response)
                except Exception as err:
                    log.warning("Fetch next_uri response with error: {}".format(
                        str(err)))
                continue
            break
        if response['next_uri'] is not None:
            log.warning(
                "after waited for 12 secs, query still not finished (next url not none)!"
            )

        if self._session is None:
            if response is not None and "session_id" in response:
                self._session = {"id": response["session_id"]}
        return response_list

    def fetch_all(self, statement):
        resp_list = self.query_with_session(statement)
        if len(resp_list) == 0:
            log.warning("fetch all with empty results")
            return None
        self._query_option = get_query_options(resp_list[0])  # record schema
        data_list = list()
        for response in resp_list:
            data = get_result(response)
            if len(data) != 0:
                data_list.extend(data)
        return data_list

    def get_query_option(self):
        return self._query_option


# if __name__ == '__main__':
#     from config import http_config
#     connector = HttpConnector()
#     connector.connect(**http_config)
#     connector.query_without_session("show databases;")
