#!/usr/bin/env python

import os
import glob
import json
import logging

QUERY_LOGS_DIR = "./databend/vector/query"
PROFILE_LOGS_DIR = "./databend/vector/profile"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def check_queries(now: str):
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
        assert query["query_text"] == f"select {now}"


def check_profiles(now: str):
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
    now = os.environ.get("NOW")
    check_queries(now)
    check_profiles(now)
