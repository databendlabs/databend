# HTTP query-token authentication

Tracking discussion: <https://github.com/databendlabs/databend/issues/21043>.

## Protocol

Submit SQL using existing Basic, JWT, key-pair or native session authentication.
A created query returns an opaque token in `X-DATABEND-QUERY-TOKEN`, for both JSON
and Arrow results. Neither the JSON body nor Arrow schema metadata contains a
`query_token` field. Clients detect support by the response header; older clients
may ignore it and continue using existing authentication.

```http
HTTP/1.1 200 OK
X-DATABEND-QUERY-ID: <query-id>
X-DATABEND-QUERY-TOKEN: <query-token>
Cache-Control: no-store
Access-Control-Expose-Headers: X-DATABEND-QUERY-ID, X-DATABEND-QUERY-TOKEN
```

Save the query ID and token before reading the response body. For follow-ups:

```http
GET /v1/query/<query-id>/page/1
Authorization: Bearer <query-token>
X-DATABEND-AUTH-METHOD: query-token
```

Query-token authentication is restricted to:

| Method | Endpoint |
| --- | --- |
| GET | `/v1/query/:id` |
| GET | `/v1/query/:id/page/:page_no` |
| GET or POST | `/v1/query/:id/final` |
| GET or POST | `/v1/query/:id/kill` |

It cannot submit SQL, log in/out, refresh sessions, heartbeat, upload data or
access metadata APIs. Session/temporary-table keepalive uses existing credentials;
query-token requests do not refresh the client session's Meta handle.

Only initial submission and eligible submission retries return the token header.
State, page (including page zero/retries), final and kill never return it. An absent
header on a follow-up must not clear the client's saved token.

## Ownership, routing and retries

The token is scoped to one query and its authenticated tenant/user. Request-body
settings such as `sandbox_tenant` may change the execution tenant, not the owner.
The route's query ID selects the query; `X-DATABEND-QUERY-ID` cannot override it.

Retain existing routing headers. Sticky forwarding precedes token validation on
the owning node; the expected-node header checks the destination, not ownership.
In management mode, an explicit tenant header must match the owner. Otherwise,
tenant headers follow the existing validation rules; headers ignored by Basic
in sandbox-enabled configurations are also ignored by query-token authentication.
An omitted tenant header is resolved from the validated query.

If the query has a client session, every query-token follow-up requires its
original cookie/session headers. Existing credentials may read state or kill a
same-owner query across sessions, but page/final require the original session.

A normally authenticated submission retry with the same client query ID may
return the retained page zero for the same tenant/user. Only a matching original
client session receives the token again. Another session may obtain the existing
result without the token; it is not automatically switched to the original session.

Losing the body after receiving headers still allows token-authenticated follow-ups.
Losing the entire response does not guarantee recovery. If it established the first
session, a retry may return page zero without restoring that session or its token.
Establish a session before submission when reliable session-bound retries are
needed. Do not automatically re-execute SQL when query state or its replayable
response is lost; it may already have run.

## Lifetime and errors

Each query has a random 256-bit token, validated without user/policy/token lookups
in Meta. New SQL still requires normal authentication and authorization. Tokens
remain valid after the user's credentials change or the user is deleted; this is
query-scoped access, not immediate per-request user revocation.

There is no independent token TTL or refresh endpoint. The token lasts as long as
the retained in-memory query record, including its closed retention period. It
does not reopen closed result pages. Removal, expiry or node restart loses both
query state and token; cross-node recovery and SQL replay are not provided.

If initial page collection fails after registration, the server closes the new
query and releases its result resources before returning the error, rather than
leaving an active query without client credentials. This does not undo SQL that
has already committed and is not a signal to automatically re-execute it.

- Missing node-local query state returns **404**, not a reason to refresh credentials.
- Invalid credentials, wrong tokens or ownership/session mismatches return **401**.
- A query absent during ordinary authentication remains **404** if inserted before
  the handler runs; that request must not access the newly inserted query.
- Query-ID existence is not hidden: a wrong token for a retained query gives 401,
  while a well-formed query-token request without retained query state gives 404.
- Duplicate `X-DATABEND-AUTH-METHOD` headers are rejected on authenticated endpoints.

## Credential binding for existing clients

Clients that do not use query tokens get the same query-scoped behavior from a
credential binding. On submission the server stores an HMAC (per-process random key)
of the requested tenant, `X-DATABEND-AUTH-METHOD` and the exact `Authorization`
header in the query record; the credential itself is not retained.

A state/page/final/kill request without the explicit `query-token` method whose
binding matches the target query is authenticated as the query owner without user,
password-policy or token lookups in Meta, so it is not a login attempt and cannot
count towards password lockout. Any other credential falls back to ordinary
authentication and ownership checks. An explicit `query-token` request never falls
back: a wrong token is 401 even if `Authorization` equals the submission credential.

The binding has the token's lifetime and revocation semantics. The submission
credential keeps working for its own query after a password change, credential
expiry or user deletion, but cannot submit new SQL. Credentials that change on
every request, such as key-pair JWTs, never match and keep the existing behavior.

## Transport and secrecy

Use TLS. Never put tokens in URLs or unredacted logs/traces. Token-bearing responses
use `Cache-Control: no-store`; server debug formatting and request-header logging
redact tokens. Proxies/APM must independently redact `Authorization` and the new
response header: an in-process sensitive flag does not travel over HTTP.

The query server emits `Access-Control-Expose-Headers`, but does not enable CORS
origins or credentials. Browser deployments must configure gateway/edge CORS and
preserve exposure of both query headers. Header delivery alone does not establish
safe caching, logging or cross-origin behavior.
