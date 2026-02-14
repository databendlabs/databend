#!/usr/bin/env python3

import argparse
import glob
import json
import logging

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
    queries = [query for query in queries if sql in query["query_text"]]
    logger.info("queries count: %d", len(queries))
    assert len(queries) == 2
    for query in queries:
        assert query["tenant"] == "test_tenant"
        assert query["qkey1"] == "qvalue1"
        assert query["qkey2"] == "qvalue2"
        assert query["query_text"] == sql


def check_profiles(keyword: str):
    profiles = []
    for filename in glob.glob(PROFILE_LOGS_DIR + "/*.log"):
        logger.info("checking profile logs: %s", filename)
        with open(filename, "r") as f:
            for line in f.readlines():
                try:
                    if keyword not in line:
                        continue
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
    argparse.add_argument("--profile-keyword", type=str, required=True)
    args = argparse.parse_args()

    check_queries(args.sql)
    check_profiles(args.profile_keyword)
