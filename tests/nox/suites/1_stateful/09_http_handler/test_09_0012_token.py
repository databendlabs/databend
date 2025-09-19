#!/usr/bin/env python3

import base64
import json
import time

import requests
from pprint import pprint

from http.cookiejar import Cookie
from requests.cookies import RequestsCookieJar

from suites.utils import comparison_output

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
query_url2 = "http://localhost:8002/v1/query"
login_url = "http://localhost:8000/v1/session/login"
logout_url = "http://localhost:8000/v1/session/logout"
renew_url = "http://localhost:8000/v1/session/refresh"
verify_url = "http://localhost:8000/v1/verify"
auth = ("root", "")


class GlobalCookieJar(RequestsCookieJar):
    def __init__(self):
        super().__init__()

    def set_cookie(self, cookie: Cookie, *args, **kwargs):
        cookie.domain = ""
        cookie.path = "/"
        super().set_cookie(cookie, *args, **kwargs)


client = requests.session()
client.cookies = GlobalCookieJar()
client.cookies.set("cookie_enabled", "true")


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
    response = client.post(
        login_url,
        auth=auth,
        headers={"Content-Type": "application/json"},
        json=payload,
    )
    return response


@print_error
def do_logout(_case_id, session_token):
    response = client.post(
        logout_url,
        headers={"Authorization": f"Bearer {session_token}"},
    )
    return response


def do_verify(session_token):
    for token in [session_token, "xxx"]:
        print("---- verify token ", token)
        response = client.get(
            verify_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        print(response.status_code)
        print(response.text)

    for a in [auth, ("u", "p")]:
        print("---- verify password: ", a)
        response = client.post(
            verify_url,
            auth=a,
        )
        print(response.status_code)
        print(response.text)

    print("---- verify no auth header ", token)
    response = client.get(
        verify_url,
    )
    print(response.status_code)
    print(response.text)


@print_error
def do_refresh(_case_id, refresh_token, session_token):
    payload = {"session_token": session_token}
    response = client.post(
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
    response = client.post(
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
    return (
        "bend-v1-"
        + ty
        + "-"
        + base64.b64encode(json.dumps(expired_claim).encode("utf-8")).decode("utf-8")
    )


@comparison_output(
    """ 
---- do_login()
200
['session_id', 'tokens', 'version']
---- do_query('select 1',)
200
[['1']]
False
False
---- do_query('select count(*) from system.clusters',)
200
---- do_query("select 'cluster'",)
200
[['cluster']]
---- do_query('CREATE TEMP TABLE t(c1 int)',)
200
True
True
---- do_query('drop TABLE t',)
200
False
False
---- do_query('select 2',)
401
{'code': 5100,
 'message': '[AUTH] JWT authentication failed: JWT auth is not configured on '
            'this server'}
---- do_query('select 3',)
401
{'code': 5100,
 'message': '[HTTP-SESSION] Failed to decode token: base64 decode error: '
            'Invalid padding, token: bend-v1-s-xxx'}
---- do_query('select 4',)
401
{'code': 5101,
 'message': '[HTTP-SESSION] Authentication failed: session token has expired'}
---- do_query('select 5',)
401
{'code': 5100,
 'message': '[HTTP-SESSION] Authentication error: incorrect token type for '
            'this endpoint'}
---- do_refresh(1,)
200
['tokens']
---- do_query('select 6',)
200
[['6']]
---- do_query('select 7',)
200
[['7']]
---- do_refresh(2,)
401
{'code': 5100,
 'message': '[AUTH] JWT authentication failed: JWT auth is not configured on '
            'this server'}
---- do_refresh(3,)
401
{'code': 5100, 'message': "invalid token type 'x'"}
---- do_refresh(4,)
401
{'code': 5102,
 'message': '[HTTP-SESSION] Authentication failed: refresh token has expired'}
---- do_refresh(5,)
401
{'code': 5100,
 'message': '[HTTP-SESSION] Authentication error: incorrect token type for '
            'this endpoint'}
---- do_refresh(6,)
200
---- do_logout(0,)
401
{'code': 5100,
 'message': '[HTTP-SESSION] Authentication error: incorrect token type for '
            'this endpoint'}
---- do_logout(1,)
200
---- do_query("select 'after logout'",)
401
{'code': 5103,
 'message': '[HTTP-SESSION] Authentication failed: session token not found in '
            'database'}
---- do_refresh('after_logout',)
401
{'code': 5104,
 'message': '[HTTP-SESSION] Authentication failed: refresh token not found in '
            'database'}
"""
)
def test_token():
    login_resp = do_login()
    pprint(sorted(login_resp.keys()))
    session_token = login_resp.get("tokens").get("session_token")
    refresh_token = login_resp.get("tokens").get("refresh_token")
    # print(session_token)

    # ok
    query_resp = do_query("select 1", session_token)
    pprint(query_resp.get("data"))
    pprint(query_resp.get("session").get("need_sticky"))
    pprint(query_resp.get("session").get("need_keep_alive"))

    # cluster
    query_resp = do_query("select count(*) from system.clusters", session_token)
    num_nodes = int(query_resp.get("data")[0][0])
    url = query_url
    if num_nodes > 1:
        url = query_url2
    query_resp = do_query("select 'cluster'", session_token, url)
    pprint(query_resp.get("data"))

    # temp table
    query_resp = do_query("CREATE TEMP TABLE t(c1 int)", session_token)
    pprint(query_resp.get("session").get("need_sticky"))
    pprint(query_resp.get("session").get("need_keep_alive"))

    query_resp = do_query("drop TABLE t", session_token)
    pprint(query_resp.get("session").get("need_sticky"))
    pprint(query_resp.get("session").get("need_keep_alive"))

    # errors
    do_query("select 2", "xxx")
    do_query("select 3", "bend-v1-s-xxx")
    do_query("select 4", fake_expired_token("s"))
    do_query("select 5", refresh_token)

    renew_resp = do_refresh(1, refresh_token, session_token)
    pprint(sorted(renew_resp.keys()))
    new_session_token = renew_resp.get("tokens").get("session_token")
    new_refresh_token = renew_resp.get("tokens").get("refresh_token")

    # old session_token still valid
    query_resp = do_query("select 6", session_token)
    pprint(query_resp.get("data"))

    query_resp = do_query("select 7", new_session_token)
    pprint(query_resp.get("data"))

    # errors
    do_refresh(2, "xxx", session_token)
    do_refresh(3, "bend-v1-xxx", session_token)
    do_refresh(4, fake_expired_token("r"), session_token)
    do_refresh(5, session_token, session_token)

    # test new_refresh_token works
    do_refresh(6, new_refresh_token, session_token)

    do_logout(0, new_refresh_token)
    do_logout(1, new_session_token)

    do_query("select 'after logout'", new_session_token)
    do_refresh("after_logout", new_refresh_token, session_token)
