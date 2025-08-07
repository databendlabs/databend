#!/usr/bin/env python3

import base64
import json
import time

import requests
from pprint import pprint

from requests import Response

HEADER_SESSION = "X-DATABEND-SESSION"
# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
login_url = "http://localhost:8000/v1/session/login"
logout_url = "http://localhost:8000/v1/session/logout"
auth = ("root", "")


def check(func):
    def wrapper(self, *args, **kwargs):
        print(f"---- {func.__name__}{args[:1]}")
        resp: Response = func(self, *args, **kwargs)
        self.session_header = resp.headers.get(HEADER_SESSION)
        last = self.session_header_json
        self.session_header_json = json.loads(
            base64.urlsafe_b64decode(self.session_header)
        )
        if last:
            if last["id"] != self.session_header_json["id"]:
                print(
                    "error: session id should not change",
                    last,
                    self.session_header_json,
                )
            if last["last_refresh_time"] < time.time() - 100:
                if last["last_refresh_time"] > time.time() - 2:
                    print("error: last_refresh_time should not change")
            else:
                if (
                    last["last_refresh_time"]
                    != self.session_header_json["last_refresh_time"]
                ):
                    print("error: last_refresh_time should not change")

        # print("get header: ", self.session_header_json)
        if not self.session_header:
            print("missing session headers:", resp.headers)
        print(resp.status_code)
        if len(resp.cookies.items()) != 0:
            print(f"unexpected cookies: {resp.cookies}")
        resp = resp.json()
        err = resp.get("error")
        if err:
            pprint(err)
        return resp

    return wrapper


class Client(object):
    def __init__(self):
        self.client = requests.session()
        self.session_header = ""
        self.session_header_json = None

    @check
    def login(self):
        payload = {}
        response = self.client.post(
            login_url,
            auth=auth,
            headers={
                "Content-Type": "application/json",
                "X-DATABEND-CLIENT-CAPS": "session_header",
            },
            json=payload,
        )
        return response

    @check
    def do_logout(self, _case_id):
        response = self.client.post(
            logout_url,
            auth=auth,
            headers={HEADER_SESSION: self.session_header},
        )
        return response

    @check
    def do_query(self, query, url=query_url):
        query_payload = {"sql": query, "pagination": {"wait_time_secs": 11}}
        response = self.client.post(
            url,
            auth=auth,
            headers={
                "Content-Type": "application/json",
                HEADER_SESSION: self.session_header,
            },
            json=query_payload,
        )
        return response

    def set_fake_last_refresh_time(self):
        j = self.session_header_json
        j["last_refresh_time"] = int(time.time()) - 10 * 60
        self.session_header = base64.urlsafe_b64encode(
            json.dumps(j).encode("utf-8")
        ).decode("ascii")


def main():
    client = Client()
    client.login()

    time.sleep(2)

    query_resp = client.do_query("select 1")
    pprint(query_resp.get("data"))
    pprint(query_resp.get("session").get("need_sticky"))
    pprint(query_resp.get("session").get("need_keep_alive"))

    client.set_fake_last_refresh_time()
    time.sleep(2)

    # temp table
    query_resp = client.do_query("CREATE OR REPLACE TEMP TABLE t(c1 int)")
    pprint(query_resp.get("session").get("need_sticky"))
    pprint(query_resp.get("session").get("need_keep_alive"))

    time.sleep(2)
    query_resp = client.do_query("drop TABLE t")
    pprint(query_resp.get("session").get("need_sticky"))
    pprint(query_resp.get("session").get("need_keep_alive"))


if __name__ == "__main__":
    import logging

    try:
        main()
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
