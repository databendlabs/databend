#!/usr/bin/env python3

import time
import json
import base64

import jwt
import requests
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

query_url = "http://localhost:8000/v1/query"
login_url = "http://localhost:8000/v1/session/login"
root_auth = ("root", "")


def sql(statement, auth=root_auth, headers=None):
    """Execute SQL via HTTP query endpoint with basic auth."""
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    resp = requests.post(
        query_url,
        auth=auth,
        headers=hdrs,
        json={"sql": statement, "pagination": {"wait_time_secs": 11}},
    )
    resp.raise_for_status()
    result = resp.json()
    assert result.get("error") is None, f"SQL error: {result['error']}"
    return result


def generate_ec_key_pair():
    """Generate an EC P-256 key pair, return (private_key, public_key_base64)."""
    private_key = ec.generate_private_key(ec.SECP256R1())
    pub_der = private_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    pub_b64 = base64.b64encode(pub_der).decode("ascii")
    return private_key, pub_b64


def sign_jwt(private_key, username):
    """Sign a JWT for key-pair auth."""
    now = int(time.time())
    payload = {
        "sub": username,
        "iat": now,
        "exp": now + 60,
    }
    return jwt.encode(payload, private_key, algorithm="ES256")


def test_key_pair_auth():
    private_key, pub_b64 = generate_ec_key_pair()
    username = "kp_test_login"

    # Setup: create key-pair user
    sql(f"DROP USER IF EXISTS '{username}'")
    sql(f"CREATE USER '{username}' IDENTIFIED WITH key_pair BY '{pub_b64}'")

    try:
        token = sign_jwt(private_key, username)

        # Login with key-pair JWT
        resp = requests.post(
            login_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
                "X-DATABEND-AUTH-METHOD": "keypair",
                "X-DATABEND-CLIENT-CAPS": "session_header",
            },
            json={},
        )
        assert resp.status_code == 200, f"login failed: {resp.status_code} {resp.text}"
        login_resp = resp.json()
        assert "tokens" in login_resp, f"no tokens in login response: {login_resp}"
        session_token = login_resp["tokens"]["session_token"]

        # Execute a query with the session token
        resp = requests.post(
            query_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {session_token}",
            },
            json={"sql": "SELECT current_user()", "pagination": {"wait_time_secs": 11}},
        )
        assert resp.status_code == 200, f"query failed: {resp.status_code} {resp.text}"
        result = resp.json()
        assert result.get("error") is None, f"query error: {result['error']}"
        assert result["data"][0][0] == f"'{username}'@'%'"

        # Query directly with key-pair JWT (no login)
        token2 = sign_jwt(private_key, username)
        resp = requests.post(
            query_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token2}",
                "X-DATABEND-AUTH-METHOD": "keypair",
            },
            json={"sql": "SELECT 42", "pagination": {"wait_time_secs": 11}},
        )
        assert resp.status_code == 200, f"direct query failed: {resp.status_code} {resp.text}"
        result = resp.json()
        assert result.get("error") is None, f"direct query error: {result['error']}"
        assert result["data"][0][0] == "42"

    finally:
        sql(f"DROP USER IF EXISTS '{username}'")
