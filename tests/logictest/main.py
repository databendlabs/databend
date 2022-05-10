from mysql_runner import TestMySQL
from http_runner import TestHttp

config = {
    'user': 'root',
    'password': 'root',
    'host': '127.0.0.1',
    "port": 3307,
    'database': 'default',
    'raise_on_warnings': True
}

http_config = {
    'host': '127.0.0.1',
    "port": 8000,
    'database': 'default',
}

if __name__ == '__main__':
    mySQL = TestMySQL("mysql")
    mySQL.set_driver(config)
    mySQL.set_label("mysql")
    mySQL.run_sql_suite()

    http = TestHttp("http")
    http.set_driver(http_config)
    http.set_label("http")
    http.run_sql_suite()

