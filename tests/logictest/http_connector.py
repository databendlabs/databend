import json
import logging

import requests
from mysql.connector.errors import Error

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def format_result(response):
    # only for debug 
    log.debug("error: {}".format(response['data'] ))
    res = ""
    for line in response['data']:
        lineTmp = ""
        for item in line:
            if lineTmp == "":
                lineTmp = str(item)
            else:
                lineTmp = lineTmp + " " + str(item)  # every item seperate by space
        res = res + lineTmp + "\n"
    return res

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
    wrapMsg = "errno:{},msg:{}".format(response['error']['code'],response['error']['message'])
    return Error(msg=wrapMsg, errno=response['error']['code'])


class HttpConnector():
    # Databend http hander details: https://databend.rs/doc/reference/api/rest

    # Call connect(**driver) 
    # driver is a dict contains:
    # {
    #   'user': 'root',
    #   'password': 'root',
    #   'host': '127.0.0.1',
    #   'port': 3307,
    #   'database': 'default'
    # }
    def connect(self, host, port, database = "default"):
        self._host = host
        self._port = port 
        self._database = database
        self._session_max_idle_time = 300
        self._session = None

    def query(self, statement, session=None ):
        url = "http://{}:{}/v1/query/".format(self._host, self._port)
        query_sql = {
            'sql': statement
        }
        if session is not None:
            query_sql['session'] = session
    
        response = requests.post(
            url,
            data=json.dumps(query_sql), 
            headers=headers
        )
        return json.loads(response.content)

    def query_without_session(self, statement):
        return self.query(statement)

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
        if "session_id" in response:
            self._session = {
                "id": response["session_id"]
            }
        return response 

    # get all result by next_uri 
    def fetch_all(self, statement):
        full_result = []
        resp = self.query_with_session(statement)
        while True:
            full_result = full_result + get_result(resp) 
            nextUri = get_next_uri(resp)
            if nextUri is None:
                break           
            resp = requests.get(nextUri)                  
        return full_result


if __name__ == '__main__':
    connector = HttpConnector()
    connector.connect("127.0.0.1", 8000)
    resp = connector.query_with_session("show databases;")

    print(format_result(resp))

    resp = connector.query_with_session("show tables;")
    print(format_result(resp))

    resp = connector.fetch_all("select * from t3;")
    print(resp)