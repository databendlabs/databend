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

clickhouse_config = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    "port": 8124,
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

    clickhouse_host = os.getenv("QUERY_CLICKHOUSE_HANDLER_HOST")
    if clickhouse_host is not None:
        clickhouse_config['host'] = clickhouse_host

    clickhouse_port = os.getenv("QUERY_CLICKHOUSE_HANDLER_PORT")
    if clickhouse_port is not None:
        clickhouse_config['port'] = clickhouse_port

    clickhouse_password = os.getenv("QUERY_CLICKHOUSE_HANDLER_PASSWORD")
    if clickhouse_password is not None:
        clickhouse_config['password'] = clickhouse_password

    clickhouse_user = os.getenv("QUERY_CLICKHOUSE_HANDLER_USER")
    if clickhouse_user is not None:
        clickhouse_config['user'] = clickhouse_user

    mysql_database = os.getenv("MYSQL_DATABASE")
    if mysql_database is not None:
        mysql_config['database'] = mysql_database
        http_config['database'] = mysql_database
        clickhouse_config['database'] = mysql_database

    mysql_user = os.getenv("MYSQL_USER")
    if mysql_user is not None:
        mysql_config['user'] = mysql_user
        http_config['user'] = mysql_user


config_from_env()
