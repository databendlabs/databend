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

def compare(releaser, pull):
    fs = [f for f in os.listdir(releaser) if f.endswith(".json")]
    fs_new = [f for f in os.listdir(pull) if f.endswith(".json")]

    files = list(set(fs).intersection(set(fs_new)))
    for name in files:
        compare_suit(releaser, pull , name)

    report(releaser, pull, files)

    print("Faster: {}, Slower: {}, Stable: {}".format(faster, slower, stable))

    if slower >= 1:
        return -1
    return 0

def compare_suit(releaser, pull, name):
    global faster
    global slower
    global stable
    global stats

    r = {}
    p = {}

    with open(os.path.join(releaser, name)) as json_file:
        releaser_result = json.load(json_file)

    with open(os.path.join(pull, name)) as json_file:
        pull_result = json.load(json_file)

    diff = pull_result["statistics"]["MiBPS"] - releaser_result["statistics"]["MiBPS"]

    stats[name] = {
        "state" : "stable",
        "diff" : diff
    }

    if abs(diff) / releaser_result["statistics"]["MiBPS"] >= 0.05:
        if diff > 0:
            faster += 1
            stats[name]['state'] = 'faster'
        else:
            slower += 1
            stats[name]['state'] = 'slower'
    else:
        stable += 1

def report(releaser, pull, files):
    ## todo render the compartions via html template
    global stats

    for name in stats:
        state = stats[name]
        print("name: {}, stat: {}, diff: {}".format(name, state['state'],  state['diff']))


## python compare.py -r xxxx -p xxxx
if __name__ == '__main__':
    parser = ArgumentParser(description='fuse perf results compare tools')
    parser.add_argument('-r', '--releaser', help='Perf results directory from release version')
    parser.add_argument('-p', '--pull',  help='Perf results directory from current build')

    args = parser.parse_args()
    code = compare(args.releaser, args.pull)

    sys.exit(code)
