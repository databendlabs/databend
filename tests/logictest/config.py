import os

mysql_config = {
    'user': 'root',
    'host': '127.0.0.1',
    "port": 3307,
    'database': 'default',
    'raise_on_warnings': True
}

http_config = {
    'user': 'root',
    'host': '127.0.0.1',
    "port": 8000,
    'database': 'default',
}


def config_from_env():
    mysql_host = os.getenv("QUERY_MYSQL_HANDLER_HOST")
    if mysql_host is not None:
        mysql_config['host'] = mysql_host

    mysql_port = os.getenv("QUERY_MYSQL_HANDLER_PORT")
    if mysql_port is not None:
        mysql_config['port'] = int(mysql_port)

    http_host = os.getenv("QUERY_HTTP_HANDLER_HOST")
    if http_host is not None:
        http_config["host"] = http_host

    http_port = os.getenv("QUERY_HTTP_HANDLER_PORT")
    if http_port is not None:
        http_config['port'] = int(http_port)

    mysql_database = os.getenv("MYSQL_DATABASE")
    if mysql_database is not None:
        mysql_config['database'] = mysql_database
        http_config['database'] = mysql_database

    mysql_user = os.getenv("MYSQL_USER")
    if mysql_user is not None:
        mysql_config['user'] = mysql_user
        http_config['user'] = mysql_user


config_from_env()
