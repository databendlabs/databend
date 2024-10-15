#!/usr/bin/env python3

import requests
import json

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
auth = ("root", "")

def send_sql(query):
    query_payload = {"sql": query, "pagination": {"page_size": 10, "page": 1}}
    response = requests.post(
        query_url,
        auth=auth,
        headers={"Content-Type": "application/json"},
        json=query_payload,
    )
    response_data = response.json()
    data_fragments = response_data.get("data")
    data_json_string = ''.join([item[0] for item in data_fragments])
    profiles = json.loads(data_json_string)
    return profiles

def judge(profile):
    print(len(profile["profiles"]))
    memory_usage, error_count, cpu_time = 0, 0, 0
    for item in profile["profiles"]:
        error_count += len(item["errors"])
        memory_usage += item["statistics"][16]
        cpu_time += item["statistics"][0]

    print(memory_usage)
    print(cpu_time > 0)
    print(error_count)


def test(query):
    profile = send_sql(query)
    judge(profile)


if __name__ == "__main__":
    test("EXPLAIN ANALYZE GRAPHICAL SELECT 1")
    test("EXPLAIN ANALYZE GRAPHICAL SELECT sleep(4)")
    test("EXPLAIN ANALYZE GRAPHICAL SELECT max(number) FROM numbers_mt (10) where number > 99999999998")
    test(
        "EXPLAIN ANALYZE GRAPHICAL SELECT max(number) FROM numbers_mt (10) WHERE number > 99999999998 GROUP BY number % 3"
    )
    send_sql("drop table if exists tbl_01_0014 all")
    send_sql("CREATE TABLE tbl_01_0014 (test VARCHAR)")
    test("EXPLAIN ANALYZE GRAPHICAL select test from tbl_01_0014")
