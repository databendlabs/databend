#!/usr/bin/env python

import os
import glob
import json
import logging
import argparse

QUERY_LOGS_DIR = ".databend/vector/query"
PROFILE_LOGS_DIR = ".databend/vector/profile"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def check_queries(sql: str):
    queries = []
    for filename in glob.glob(QUERY_LOGS_DIR + "/*.log"):
        logger.info("checking query logs: %s", filename)
        with open(filename, "r") as f:
            for line in f.readlines():
                try:
                    query = json.loads(line)
                    queries.append(query)
                except Exception:
                    pass

    logger.info("queries count: %d", len(queries))
    assert len(queries) == 2
    for query in queries:
        assert query["tenant"] == "test_tenant"
        assert query["qkey1"] == "qvalue1"
        assert query["qkey2"] == "qvalue2"
        assert query["query_text"] == sql


def check_profiles():
    profiles = []
    for filename in glob.glob(PROFILE_LOGS_DIR + "/*.log"):
        logger.info("checking profile logs: %s", filename)
        with open(filename, "r") as f:
            for line in f.readlines():
                try:
                    query = json.loads(line)
                    profiles.append(query)
                except Exception:
                    pass

    logger.info("profiles count: %d", len(profiles))
    assert len(profiles) == 1
    for profile in profiles:
        assert profile["tenant"] == "test_tenant"
        assert profile["pkey1"] == "pvalue1"
        assert profile["pkey2"] == "pvalue2"
        assert profile["source_type"] == "opentelemetry"


if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--sql", type=str)
    args = argparse.parse_args()
    logger.info("sql: %s", args.sql)
    check_queries(args.sql)
    check_profiles()
