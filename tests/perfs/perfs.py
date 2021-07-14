#!coding: utf-8
from errno import ESRCH

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
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys
import logging

failures = 0
passed = 0

def load_config():
    with open('perfs.yaml','r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        return data

conf = load_config()

def build_COSclient(secretID, secretKey, Region):

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    secret_id = secretID
    secret_key = secretKey
    region = Region
    token = None         # TODO(zhihanz) support token for client
    scheme = 'https'
    config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
    client = CosS3Client(config)
    return client

def execute(suit, bin_path, host, port, concurrency, iterations, output_dir, type, region, bucket, S3path, secretID, secretKey):
    base_cfg = conf['config']
    if iterations == "" :
        iterations = suit.get("iterations", base_cfg['iterations'])
    if concurrency == "":
        concurrency = suit.get("concurrency", base_cfg['concurrency'])
    if bin_path == "":
        print("you should specific path for fuse-benchmark binary file")
        return
    suit_name = re.sub(r"\s+", '-', suit['name'])
    file_name = "{}-result.json".format(suit_name)
    json_path = os.path.join(output_dir, file_name)

    command = '{} -c {} -i {} -h {} -p {} --query "{}" --json "{}" '.format(bin_path, concurrency, iterations, host, port, suit['query'], json_path)
    print("perf {}, query: {} \n".format(suit_name, suit['query']))

    proc = Popen(command, shell=True, env=os.environ)
    start_time = datetime.now()
    while proc.poll() is None:
        sleep(0.01)
    total_time = (datetime.now() - start_time).total_seconds()
    if type == "COS":
        COScli = build_COSclient(secretID, secretKey, region)
        with open(json_path, 'rb') as fp:
            response = COScli.put_object(
                Bucket=bucket,
                Body=fp,
                Key='{}/{}'.format(S3path, file_name),
                StorageClass='STANDARD',
                EnableMD5=False
            )
            print(response['ETag'])

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
    parser.add_argument('--host', default = "127.0.0.1",  help='Clickhouse handler Server host')
    parser.add_argument('-p', '--port', default = "9001",  help='Clickhouse handler Server port')
    parser.add_argument('-c', '--concurrency', default = "",  help='Set default concurrency for all perf tests')
    parser.add_argument('-i', '--iteration', default = "",  help='Set default iteration number for each performance tests to run')
    parser.add_argument('-t', '--type', default = "local",  help='Set storage endpoint for performance testing, support local and COS')
    parser.add_argument('--region', default = "",  help='Set storage region')
    parser.add_argument('--bucket', default = "",  help='Set storage bucket')
    parser.add_argument('--path', default = "",  help='Set absolute path to store objects')
    parser.add_argument('--secretID', default = "",  help='Set storage secret ID')
    parser.add_argument('--secretKey', default = "",  help='Set storage secret Key')
    args = parser.parse_args()

    for suit in conf['perfs']:
        execute(suit, args.bin, args.host, args.port, args.concurrency, args.iteration, args.output,
                args.type, args.region, args.bucket, args.path, args.secretID, args.secretKey)
