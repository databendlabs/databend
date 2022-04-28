from tests.logictest.mysql_runner import TestMySQL

config = {
    'user': 'root',
    'password': 'root',
    'host': '127.0.0.1',
    "port": 3307,
    'database': 'default',
    'raise_on_warnings': True
}
if __name__ == '__main__':
    mySQL = TestMySQL("mysql")
    mySQL.set_driver(config)
    mySQL.set_label("mysql")
    mySQL.run_sql_suite()
