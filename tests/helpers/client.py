import os
import sys
import time

import subprocess
from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError
from subprocess import TimeoutExpired

CURDIR = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0, os.path.join(CURDIR))


class client(object):

    def __init__(self, name='', log=None):
        self.name = name
        self.log = log
        self.client = f'mysql --user default -s'
        tcp_host = os.getenv("QUERY_MYSQL_HANDLER_HOST")
        if tcp_host is not None:
            self.client += f' --host={tcp_host}'
        else:
            self.client += f' --host=127.0.0.1'

        tcp_port = os.getenv("QUERY_MYSQL_HANDLER_PORT")
        if tcp_port is not None:
            self.client += f" --port={tcp_port}"
        else:
            self.client += f" --port=3307"

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

    def run(self, sqls):
        p = Popen(self.client, shell=True, stdin=PIPE, universal_newlines=True)
        p.communicate(input=sqls)[0]

    def run_with_output(self, sqls):
        p = Popen(self.client,
                  shell=True,
                  stdin=PIPE,
                  stdout=PIPE,
                  universal_newlines=True)
        return p.communicate(input=sqls)


if __name__ == '__main__':
    client = client("e")
    client.run("create table test2(a int);")
