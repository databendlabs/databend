#!coding: utf-8

import yaml
import re
import subprocess
import os
import sys
import json

from datetime import datetime
from time import time, sleep

from argparse import ArgumentParser

faster = 0
slower = 0
stable = 0

stats = {}

def load_config():
    with open('perfs.yaml','r') as f:
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
            <th>before_score,&nbsp;s</th>
            <th>after_score,&nbsp;s</th>
            <th>Ratio of speedup&nbsp;(-) or slowdown&nbsp;(+)</th>
            <th>Relative difference (after_score&nbsp;−&nbsp;before_score) / before_score</th>
            <th>Test</th>
            <th>Status</th>
            <th>Query</th>
            </tr>

            {}
        </tbody>
    </table>
</body>
</html> """

def create_tr(name, before_score, after_score, state, query):
    tmp = """ </tr>
        <tr id="changes-in-performance.{}">
        <td>{:.3f}</td>
        <td>{:.3f}</td>
        <td>{}{:.3f}x</td>
        <td>{:.3f}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
    </tr> """

    return tmp.format(name, before_score, after_score,  '+' if after_score > before_score else '-',  after_score/before_score, (after_score - before_score) / before_score, name, state, query)

def get_suit_by_name(name):
    for suit in conf['perfs']:
        suit_name = re.sub(r"\s+", '-', suit['name'])
        if suit_name == name:
            return suit
    return None


def compare(releaser, pull):
    fs = [f for f in os.listdir(releaser) if f.endswith(".json")]
    fs_after_score = [f for f in os.listdir(pull) if f.endswith(".json")]

    files = list(set(fs).intersection(set(fs_after_score)))
    for f in files:
        suit_name = f.replace("-result.json", "")
        compare_suit(releaser, pull, f, suit_name)

    report(releaser, pull, files)

    print("Faster: {}, Slower: {}, Stable: {}".format(faster, slower, stable))

    if slower >= 1:
        return -1
    return 0

def compare_suit(releaser, pull, suit_file, suit_name):
    global faster
    global slower
    global stable
    global stats

    r = {}
    p = {}

    with open(os.path.join(releaser, suit_file)) as json_file:
        releaser_result = json.load(json_file)

    with open(os.path.join(pull, suit_file)) as json_file:
        pull_result = json.load(json_file)

    diff = pull_result["statistics"]["MiBPS"] - releaser_result["statistics"]["MiBPS"]

    stats[suit_name] = {
        "name" : suit_name,
        "before_score" : releaser_result["statistics"]["MiBPS"],
        "after_score": pull_result["statistics"]["MiBPS"],
        "diff" : diff,
        "state" : "stable",
    }

    if abs(diff) / stats[suit_name]["before_score"] >= 0.05:
        if diff > 0:
            faster += 1
            stats[suit_name]['state'] = 'faster'
        else:
            slower += 1
            stats[suit_name]['state'] = 'slower'
    else:
        stable += 1

def report(releaser, pull, files):
    ## todo render the compartions via html template
    global stats

    trs = ""
    for name in stats:
        state = stats[name]
        suit = get_suit_by_name(name)

        trs += create_tr(name, state['before_score'], state['after_score'], state['state'], suit['query'])

    html = template.format(trs)

    with open('/tmp/performance.html','w') as f:
        f.write(html)
        f.close()



## python compare.py -r xxxx -p xxxx
if __name__ == '__main__':
    parser = ArgumentParser(description='fuse perf results compare tools')
    parser.add_argument('-r', '--releaser', help='Perf results directory from release version')
    parser.add_argument('-p', '--pull',  help='Perf results directory from current build')

    args = parser.parse_args()
    code = compare(args.releaser, args.pull)

    sys.exit(code)
