#!coding: utf-8

import yaml
import re
import subprocess
import os

from datetime import datetime
from time import time, sleep

from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError
from subprocess import TimeoutExpired
from argparse import ArgumentParser


failures = 0
passed = 0

def load_config():
    with open('perfs.yaml','r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        return data

conf = load_config()

def execute(suit, bin_path, host, port, output_dir):
    base_cfg = conf['config']

    iterations = suit.get("iterations", base_cfg['iterations'])
    concurrency = suit.get("concurrency", base_cfg['concurrency'])

    suit_name = re.sub(r"\s+", '-', suit['name'])
    json_path = os.path.join(output_dir, "{}-result.json".format(suit_name))

    command = '{} -c {} -i {} -h {} -p {} --query "{}" --json "{}" '.format(bin_path, concurrency, iterations, host, port, suit['query'], json_path)
    print("perf {}, query: {} \n".format(suit_name, suit['query']))

    proc = Popen(command, shell=True, env=os.environ)
    start_time = datetime.now()
    while proc.poll() is None:
        sleep(0.01)
    total_time = (datetime.now() - start_time).total_seconds()


    global failures
    global passed

    if proc.returncode is None:
        try:
            proc.kill()
        except OSError as e:
            if e.errno != ESRCH:
                raise

        failures += 1
    elif proc.returncode != 0:
        failures += 1
    else:
        passed += 1


if __name__ == '__main__':
    parser = ArgumentParser(description='fuse perf tests')
    parser.add_argument('-o', '--output', default = ".",  help='Perf results directory')
    parser.add_argument('-b', '--bin', default = "fuse-benchmark",  help='Fuse benchmark binary')
    parser.add_argument('--host', default = "127.0.0.1",  help='Server host')
    parser.add_argument('-p', '--port', default = "9001",  help='Server port')
    args = parser.parse_args()

    for suit in conf['perfs']:
        execute(suit, args.bin, args.host, args.port, args.output)
