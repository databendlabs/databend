#!/usr/bin/env python3

import requests
import time
import json

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
profiling_url = "http://localhost:8080/v1/queries/{}/profiling"
auth = ('root', '')

def send_sql(query):
    query_payload = {"sql": query}
    response = requests.post(query_url, auth=auth, headers={'Content-Type': 'application/json'}, json=query_payload)
    response_data = response.json()
    query_id = response_data.get('id')
    return query_id

def get_profile(query_id):
    response_data = requests.get(profiling_url.format(query_id), auth=auth)
    return response_data.json()

def judge(profile):
    print(len(profile['profiles']))
    error_count = 0
    for item in profile['profiles']:
        error_count += len(item['errors'])
    print(error_count)

def test(query):
    query_id = send_sql(query)
    profile = get_profile(query_id)
    judge(profile)

if __name__ == "__main__":
    test('SELECT 1')
    test('SELECT sleep(4)')
    test('SELECT max(number) FROM numbers_mt (10) where number > 99999999998')
    test('SELECT max(number) FROM numbers_mt (10) WHERE number > 99999999998 GROUP BY number % 3')
