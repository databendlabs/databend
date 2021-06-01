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

def compare(master, pull):
    fs = [f for f in os.listdir(master) if f.endswith(".json")]
    fs_new = [f for f in os.listdir(pull) if f.endswith(".json")]

    files = list(set(fs).intersection(set(fs_new)))
    for name in files:
        compare_suit(master, pull , name)

    print("Faster: {}, Slower: {}, Stable: {}".format(faster, slower, stable))

    if slower >= 1:
        return -1
    return 0

def compare_suit(master, pull, name):
    global faster
    global slower
    global stable

    m = {}
    p = {}

    with open(os.path.join(master, name)) as json_file:
        m = json.load(json_file)

    with open(os.path.join(pull, name)) as json_file:
        p = json.load(json_file)

    diff = p["statistics"]["MiBPS"] - m["statistics"]["MiBPS"]
    if abs(diff) / m["statistics"]["MiBPS"] >= 0.10:
        if diff > 0:
            faster += 1
        else:
            slower += 1
    else:
        stable += 1
    pass

## python compare.py -m . -p ../xxx
if __name__ == '__main__':
    parser = ArgumentParser(description='fuse perf results compare tools')
    parser.add_argument('-m', '--master', help='Perf results directory from Master build')
    parser.add_argument('-p', '--pull',  help='Perf results directory from current build')

    args = parser.parse_args()
    code = compare(args.master, args.pull)
    sys.exit(code)
