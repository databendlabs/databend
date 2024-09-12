#!/usr/bin/env python3

import base64
import json
import time

import requests
from pprint import pprint

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
query_url2 = "http://localhost:8002/v1/query"
login_url = "http://localhost:8000/v1/session/login"
logout_url = "http://localhost:8000/v1/session/logout"
renew_url = "http://localhost:8000/v1/session/refresh"
verify_url = "http://localhost:8000/v1/auth/verify"
auth = ("root", "")


def print_error(func):
    def wrapper(*args, **kwargs):
        print(f"---- {func.__name__}{args[:1]}")
        resp = func(*args, **kwargs)
        print(resp.status_code)
        resp = resp.json()
        err = resp.get("error")
        if err:
            pprint(err)
        return resp

    return wrapper


@print_error
def do_login():
    payload = {}
    response = requests.post(
        login_url,
        auth=auth,
        headers={"Content-Type": "application/json"},
        json=payload,
    )
    return response


@print_error
def do_logout(_case_id, session_token):
    response = requests.post(
        logout_url,
        headers={"Authorization": f"Bearer {session_token}"},
    )
    return response


def do_verify(session_token):
    for token in [session_token, 'xxx']:
        print("---- verify token ", token)
        response = requests.get(
            verify_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        print(response.status_code)
        print(response.text)

    for a in [auth, ('u', 'p')]:
        print("---- verify password: ", a)
        response = requests.post(
            verify_url,
            auth=a,
        )
        print(response.status_code)
        print(response.text)

    print("---- verify no auth header ", token)
    response = requests.get(
        verify_url,
    )
    print(response.status_code)
    print(response.text)


@print_error
def do_refresh(_case_id, refresh_token, session_token):
    payload = {"session_token": session_token}
    response = requests.post(
        renew_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {refresh_token}",
        },
        json=payload,
    )
    return response


@print_error
def do_query(query, session_token, url=query_url):
    query_payload = {"sql": query, "pagination": {"wait_time_secs": 11}}
    response = requests.post(
        url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {session_token}",
        },
        json=query_payload,
    )
    return response


def fake_expired_token(ty):
    expired_claim = {
        # TTL_GRACE_PERIOD_QUERY = 600
        "exp": int(time.time()) - 610,
        "tenant": "",
        "user": "",
        "nonce": "",
        "sid": "",
    }
    return "bend-v1-" + ty + '-' + base64.b64encode(
        json.dumps(expired_claim).encode("utf-8")
    ).decode("utf-8")


def main():
    login_resp = do_login()
    pprint(sorted(login_resp.keys()))
    session_token = login_resp.get("session_token")
    refresh_token = login_resp.get("refresh_token")
    # print(session_token)

    # ok
    query_resp = do_query("select 1", session_token)
    pprint(query_resp.get("data"))

    # cluster
    query_resp = do_query("select count(*) from system.clusters", session_token)
    num_nodes = int(query_resp.get("data")[0][0])
    url = query_url
    if num_nodes > 1:
        url = query_url2
    query_resp = do_query("select 'cluster'", session_token, url)
    pprint(query_resp.get("data"))

    # errors
    do_query("select 2", "xxx")
    do_query("select 3", "bend-v1-s-xxx")
    do_query("select 4", fake_expired_token('s'))
    do_query("select 5", refresh_token)

    renew_resp = do_refresh(1, refresh_token, session_token)
    pprint(sorted(renew_resp.keys()))
    new_session_token = renew_resp.get("session_token")
    new_refresh_token = renew_resp.get("refresh_token")

    # old session_token still valid
    query_resp = do_query("select 6", session_token)
    pprint(query_resp.get("data"))

    query_resp = do_query("select 7", new_session_token)
    pprint(query_resp.get("data"))

    # errors
    do_refresh(2, "xxx", session_token)
    do_refresh(3, "bend-v1-xxx", session_token)
    do_refresh(4, fake_expired_token('r'), session_token)
    do_refresh(5, session_token, session_token)

    # test new_refresh_token works
    do_refresh(6, new_refresh_token, session_token)

    do_logout(0, new_refresh_token)
    do_logout(1, new_session_token)

    do_query("select 'after logout'", new_session_token)
    do_refresh("after_logout", new_refresh_token, session_token)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
