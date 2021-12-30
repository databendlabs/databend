#!coding: utf-8
import json

from errno import ESRCH
import configargparse
import yaml
import re
import os

from datetime import datetime
from time import sleep

from subprocess import Popen
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys
import logging

from qcloud_cos.cos_exception import CosServiceError

failures = 0
passed = 0
index = {'Contents': []}


def load_config():
    with open('perfs.yaml', 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        return data


conf = load_config()


def build_COSclient(secretID, secretKey, Region, Endpoint):
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    secret_id = secretID
    secret_key = secretKey
    region = Region
    token = None  # TODO(zhihanz) support token for client
    scheme = 'http'
    config = CosConfig(Region=region,
                       SecretId=secret_id,
                       SecretKey=secret_key,
                       Token=token,
                       Scheme=scheme,
                       Domain=Endpoint)
    client = CosS3Client(config)
    return client


def save_json(client, bucket, path, output, file):
    with open(os.path.join(output, "index.json"), 'w') as outfile:
        json.dump(file, outfile)
    with open(os.path.join(output, "index.json"), 'rb') as fp:
        response = client.put_object(Bucket=bucket,
                                     Body=fp,
                                     Key=os.path.join(path, "index.json"),
                                     StorageClass='STANDARD',
                                     EnableMD5=False)
        logging.warning(response['ETag'])


def update_index(client, bucket, path, key, file_name, output):
    try:
        response = client.get_object(
            Bucket=bucket,
            Key=os.path.join(path, "index.json"),
        )
    except CosServiceError as e:
        if e.get_error_code() == 'NoSuchKey':
            new_index = {'Contents': []}
            new_index['Contents'].append({
                'path': key,
                'file_name': file_name,
            })
            save_json(client, bucket, path, output, new_index)
        else:
            logging.info("other issue occured, {}".format(e.get_error_code()))
    except ConnectionError as ce:
        logging.info("timeout during index update")
    else:
        # Update index
        new_index = json.load(response['Body'].get_raw_stream())
        new_index['Contents'].append({
            'path': key,
            'file_name': file_name,
        })
        save_json(client, bucket, path, output, new_index)


def execute(suit, bin_path, host, port, concurrency, iterations, output_dir,
            type, region, bucket, S3path, secretID, secretKey, endpoint,
            rerun):
    base_cfg = conf['config']
    if iterations == "":
        iterations = suit.get("iterations", base_cfg['iterations'])
    if concurrency == "":
        concurrency = suit.get("concurrency", base_cfg['concurrency'])
    if bin_path == "":
        logging.warning(
            "you should specific path for databend-benchmark binary file")
        return
    suit_name = re.sub(r"\s+", '-', suit['name'])
    file_name = "{}-result.json".format(suit_name)
    json_path = os.path.join(output_dir, file_name)
    S3key = os.path.join(S3path, file_name)
    if type == "COS":
        if not rerun:
            COScli = build_COSclient(secretID, secretKey, region, endpoint)
            try:
                response = COScli.get_object(
                    Bucket=bucket,
                    Key=os.path.join(S3path, "index.json"),
                )

            except CosServiceError as e:
                if e.get_error_code() == 'NoSuchResource':
                    logging.info("continue on test")
                elif e.get_error_code() == 'NoSuchKey':
                    logging.info("continue on test")
                else:
                    logging.info("other issue occured, {}".format(
                        e.get_error_code()))
            except ConnectionError as ce:
                logging.info("timeout for {}, with error {}".format(
                    S3key, str(ce)))
            else:
                # S3 key exists in given bucket just return
                index = json.load(response['Body'].get_raw_stream())
                for elem in index['Contents']:
                    if elem['path'] == S3key:
                        logging.info(
                            "S3 key {} found in bucket and not continue on it".
                            format(S3key))
                        return
    command = '{} -c {} -i {} -h {} -p {} --query "{}" --json "{}" '.format(
        bin_path, concurrency, iterations, host, port, suit['query'],
        json_path)
    logging.warning("perf {}, query: {} \n".format(suit_name, suit['query']))

    proc = Popen(command, shell=True, env=os.environ)
    start_time = datetime.now()
    while proc.poll() is None:
        sleep(0.01)
    total_time = (datetime.now() - start_time).total_seconds()
    if type == "COS":
        COScli = build_COSclient(secretID, secretKey, region, endpoint)
        with open(json_path, 'rb') as fp:
            response = COScli.put_object(Bucket=bucket,
                                         Body=fp,
                                         Key=S3key,
                                         StorageClass='STANDARD',
                                         EnableMD5=False)
            logging.warning(response['ETag'])
            update_index(COScli, bucket, S3path, S3key, file_name, output_dir)

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
    print("Total time: {}s".format(total_time))


if __name__ == '__main__':
    parser = configargparse.ArgParser()
    parser.add_argument('-o',
                        '--output',
                        default=".",
                        help='Perf results directory')
    parser.add_argument('-b',
                        '--bin',
                        default="databend-benchmark",
                        help='Databend benchmark binary')
    parser.add_argument('--host',
                        default="127.0.0.1",
                        help='Clickhouse handler Server host')
    parser.add_argument('-p',
                        '--port',
                        default="9001",
                        help='Clickhouse handler Server port')
    parser.add_argument('-c',
                        '--concurrency',
                        default="",
                        help='Set default concurrency for all perf tests')
    parser.add_argument(
        '-i',
        '--iteration',
        default="",
        help='Set default iteration number for each performance tests to run')
    parser.add_argument(
        '-t',
        '--type',
        default="local",
        help=
        'Set storage endpoint for performance testing, support local and COS')
    parser.add_argument('--region',
                        default="",
                        help='Set storage region',
                        env_var='REGION')
    parser.add_argument('--bucket',
                        default="",
                        help='Set storage bucket',
                        env_var='BUCKET')
    parser.add_argument('--path',
                        default="",
                        help='Set absolute path to store objects')
    parser.add_argument('--secretID',
                        default="",
                        help='Set storage secret ID',
                        env_var='SECRET_ID')
    parser.add_argument('--secretKey',
                        default="",
                        help='Set storage secret Key',
                        env_var='SECRET_KEY')
    parser.add_argument('--endpoint',
                        default="",
                        help='Set storage endpoint',
                        env_var='ENDPOINT')
    parser.add_argument(
        '--rerun',
        action='store_true',
        help=
        'if use `--rerun` set as true, it will rerun all perfs.yaml completely'
    )
    args = parser.parse_args()

    for suit in conf['perfs']:
        execute(suit, args.bin, args.host, args.port, args.concurrency,
                args.iteration, args.output, args.type, args.region,
                args.bucket, args.path, args.secretID, args.secretKey,
                args.endpoint, args.rerun)
