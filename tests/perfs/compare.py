#!coding: utf-8
import yaml
import re
import os
import sys
import json

from argparse import ArgumentParser
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys
import logging

faster = 0
slower = 0
stable = 0

stats = {}


def build_COSclient(secretID, secretKey, Region, Endpoint):

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    secret_id = secretID
    secret_key = secretKey
    region = Region
    config = CosConfig(Region=region,
                       SecretId=secret_id,
                       SecretKey=secret_key,
                       Scheme="https",
                       Domain=Endpoint)
    client = CosS3Client(config)
    return client


def get_perf(client, bucket, key, output):
    response = client.get_object(
        Bucket=bucket,
        Key=key,
    )
    response['Body'].get_stream_to_file(output)
    return client.get_object_url(bucket, key)


# store all S3 report files to output directory
# Problem: list_object cannot be used for global acceleration(COS)
def retrieve_all_files(client, bucket, path, output_dir):
    response = response = client.get_object(
        Bucket=bucket,
        Key=os.path.join(path, "index.json"),
    )
    index = json.load(response['Body'].get_raw_stream())
    file_dict = {}
    for elem in index['Contents']:
        print(elem)
        file_name = elem['file_name']
        u = get_perf(client, bucket, elem['path'],
                     os.path.join(output_dir, file_name))
        file_dict[file_name] = u
    return file_dict


def load_config():
    with open('perfs.yaml', 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        return data


conf = load_config()

template = """ <html>

<style>

table {{
    border: none;
    border-spacing: 0px;
    line-height: 1.5;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1);
    text-align: left;
}}

th, td {{
    border: none;
    padding: 5px;
    vertical-align: top;
    background-color: #FFF;
    font-family: sans-serif;
}}

th {{
    border-bottom: 2px solid black;
}}

tr:nth-child(odd) td {{filter: brightness(90%);}}

</style>

<body>

    <h2 id="changes-in-performance">
        <a class="cancela" >Changes in Performance</a>
    </h2>

    <table class="changes-in-performance">
        <tbody>
            <tr id="changes-in-performance.6">
            <th>{},&nbsp;s</th>
            <th>{},&nbsp;s</th>
            <th>Ratio of speedup&nbsp;(-) or slowdown&nbsp;(+)</th>
            <th>Relative difference (after_score&nbsp;&minus;&nbsp;before_score) / before_score</th>
            <th>Test</th>
            <th>Status</th>
            <th>Query</th>
            </tr>

            {}
        </tbody>
    </table>
</body>
</html> """


def create_tr(name, before_score, after_score, state, query, type,
              before_result_url, after_result_url):
    tmp = """ </tr>
        <tr id="changes-in-performance.{}">
        <td>{}</td>
        <td>{}</td>
        <td>{}{:.3f}x</td>
        <td>{:.3f}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
    </tr> """
    before = ''
    after = ''
    if type == 'local':
        before = '{:.3f}'.format(before_score)
        after = '{:.3f}'.format(after_score)
    elif type == 'COS':
        before = '<a href="{}">{:.3f}</a>'.format(before_result_url,
                                                  before_score)
        after = '<a href="{}">{:.3f}</a>'.format(after_result_url, after_score)
    if before_score == 0:
        return tmp.format(name, before, after, '?', 0, 0, name, state, query)
    return tmp.format(name, before, after,
                      '+' if after_score > before_score else '-',
                      after_score / before_score,
                      (after_score - before_score) / before_score, name, state,
                      query)


def get_suit_by_name(name):
    for suit in conf['perfs']:
        suit_name = re.sub(r"\s+", '-', suit['name'])
        if suit_name == name:
            return suit
    return None


def compare(releaser, pull, type, region, bucket, rpath, ppath, secret_id,
            secret_key, output_path, endpoint, rloglink, ploglink):
    global cli
    dict_r = {}
    dict_p = dict()
    if type == "COS":
        cli = build_COSclient(secretID=secret_id,
                              secretKey=secret_key,
                              Region=region,
                              Endpoint=endpoint)
        dict_r = retrieve_all_files(cli, bucket, rpath, releaser)
        dict_p = retrieve_all_files(cli, bucket, ppath, pull)

    fs = [f for f in os.listdir(releaser) if f.endswith(".json")]
    fs_after_score = [f for f in os.listdir(pull) if f.endswith(".json")]

    files = list(set(fs).intersection(set(fs_after_score)))
    for f in files:
        suit_name = f.replace("-result.json", "")
        compare_suit(releaser, pull, f, suit_name, type, dict_r.get(f, ''),
                     dict_p.get(f, ''))

    report(releaser, pull, files, type, rloglink, ploglink)
    if type == "COS":
        with open('/tmp/performance.html', 'rb') as fp:
            response = cli.put_object(Bucket=bucket,
                                      Body=fp,
                                      Key='{}/{}'.format(
                                          output_path, 'performance.html'),
                                      StorageClass='STANDARD',
                                      EnableMD5=False)
            print(response['ETag'])

    print("Faster: {}, Slower: {}, Stable: {}".format(faster, slower, stable))

    if slower >= 1:
        return -1
    return 0


def compare_suit(releaser, pull, suit_file, suit_name, type, releaser_suit_url,
                 pull_suit_url):
    global faster
    global slower
    global stable
    global stats

    with open(os.path.join(releaser, suit_file)) as json_file:
        releaser_result = json.load(json_file)

    with open(os.path.join(pull, suit_file)) as json_file:
        pull_result = json.load(json_file)

    diff = pull_result["statistics"]["MiBPS"] - releaser_result["statistics"][
        "MiBPS"]

    stats[suit_name] = {
        "name": suit_name,
        "before_score": releaser_result["statistics"]["MiBPS"],
        "after_score": pull_result["statistics"]["MiBPS"],
        "diff": diff,
        "state": "stable",
        "type": type,
        "before_url": "",
        "after_url": "",
    }

    if type == 'COS':
        stats[suit_name]["before_url"] = releaser_suit_url
        stats[suit_name]["after_url"] = pull_suit_url

    if stats[suit_name]["before_score"] == 0:
        stable += 1
        stats[suit_name]['state'] = 'zero divide'
        return
    if abs(diff) / stats[suit_name]["before_score"] >= 0.05:
        if diff > 0:
            faster += 1
            stats[suit_name]['state'] = 'faster'
        else:
            slower += 1
            stats[suit_name]['state'] = 'slower'
    else:
        stable += 1


def report(releaser, pull, files, type, current_log_link, ref_log_link):
    ## todo render the compartions via html template
    global stats

    trs = ""
    for name in stats:
        state = stats[name]
        suit = get_suit_by_name(name)

        trs += create_tr(name, state['before_score'], state['after_score'],
                         state['state'], suit['query'], state['type'],
                         state['before_url'], state['after_url'])
    before = "current_score"
    after = "ref_score"
    if type == 'COS':
        before = '<a href="{}">current_score</a>'.format(current_log_link)
        after = '<a href="{}">ref_score</a>'.format(ref_log_link)
    html = template.format(before, after, trs)
    with open('/tmp/performance.html', 'w') as f:
        f.write(html)
        f.close()


## python3 compare.py -r xxxx -p xxxx
if __name__ == '__main__':
    parser = ArgumentParser(description='databend perf results compare tools')
    parser.add_argument('-r',
                        '--releaser',
                        help='Perf results directory from release version')
    parser.add_argument('-p',
                        '--pull',
                        help='Perf results directory from current build')
    parser.add_argument(
        '-t',
        '--type',
        default="local",
        help='Set storage endpoint for performance testing, support '
        'local and COS')
    parser.add_argument('--region', default="", help='Set storage region')
    parser.add_argument('--bucket', default="", help='Set storage bucket')
    parser.add_argument('--rpath',
                        default="",
                        help='absolute path releaser objects')
    parser.add_argument('--ppath',
                        default="",
                        help='absolute path pull objects')
    parser.add_argument('--currentLogLink',
                        default="",
                        help='absolute path store current log')
    parser.add_argument('--refLogLink',
                        default="",
                        help='absolute path store ref log')
    parser.add_argument('--secretID', default="", help='Set storage secret ID')
    parser.add_argument('--secretKey',
                        default="",
                        help='Set storage secret Key')
    parser.add_argument('--endpoint',
                        default="",
                        help='Set accelerate endpoint for S3')
    parser.add_argument('-o',
                        '--outputPath',
                        default="",
                        help='store output in remote object storage')

    args = parser.parse_args()
    code = compare(args.releaser, args.pull, args.type, args.region,
                   args.bucket, args.rpath, args.ppath, args.secretID,
                   args.secretKey, args.outputPath, args.endpoint,
                   args.currentLogLink, args.refLogLink)

    sys.exit(code)
