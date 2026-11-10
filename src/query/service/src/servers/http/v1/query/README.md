# HTTP query-scoped authentication

Tracking discussion: <https://github.com/databendlabs/databend/issues/21043>.

## Protocol

A successfully created HTTP query returns an additional `query_token` field in
its JSON response, or in the `response_header` schema metadata for Arrow results.
Clients can detect support by the presence of this field. Older clients may
ignore it; existing Basic, JWT, key-pair and native session authentication remain
available.

Submit new queries with the normal authentication method. For subsequent state,
page, final and kill requests for that query, use:

```http
GET /v1/query/<query-id>/page/1
Authorization: Bearer <query_token>
X-DATABEND-AUTH-METHOD: query
```

Retain the existing query-node routing headers and, when the query was created
with a client session, that session's cookie or session headers. An explicit
tenant header must match the query's tenant; when omitted, the authenticated
query supplies the tenant. Authentication uses the query ID from the route,
not the caller's `X-DATABEND-QUERY-ID` header.

The mode is restricted to these endpoints:

- `GET /v1/query/:id`
- `GET /v1/query/:id/page/:page_no`
- `GET` or `POST /v1/query/:id/final`
- `GET` or `POST /v1/query/:id/kill`

It cannot submit SQL, log in/out, refresh a session, heartbeat, upload data or
access catalog/metadata APIs. Heartbeats and session/temporary-table keepalive
continue using existing credentials. Query-token requests do not refresh the
client session's Meta handle.

## State and lifetime

Each `HttpQuery` owns a random 256-bit bearer capability, checked with a
constant-time comparison. No token record is written to Meta, and validation
does not read users, policies or token records from Meta. New queries still
perform normal user authentication and SQL authorization.

There is no independent expiration timer or refresh endpoint. Tokens live as
long as the associated in-memory query record, including its existing closed
retention period. Repeated final/kill requests retain their existing behavior;
closed-query tokens do not make result pages readable again. Removal of the
query record invalidates the token. Node restart loses the query and its token;
this does not provide cross-node recovery or resumption.

Every handler checks the actual query object's tenant, user and client session
before exposing results or its token. Query-token authentication is revalidated
against that object; final/kill perform this check while holding the query map
lock, so a reused query ID cannot let a request authenticated for an old query
close a replacement query.

An authenticated retry of query submission with the same query ID can return
the existing query and token only for the same tenant, user and client session.
This does not add automatic SQL replay or guarantee recovery if the query record
has already disappeared.

## Security and credential changes

The token authorizes an already-authorized query, not the user's future SQL.
An existing query remains accessible after that user's password changes or
the user is dropped. This is intentional capability-based authorization, not
immediate per-request user revocation. New SQL still requires valid credentials.

Use TLS. Never put the token in a URL or log/trace an unredacted response body.
Server `Debug` formatting redacts the token, and authentication errors do not
include it. The token is transmitted in the standard Authorization header,
which existing request-header sanitization already redacts.

For self-password changes, a future client implementation can retain the new
password from its parsed SQL, fetch terminal success using this query token,
and only then replace its local credentials. This server change does not echo
passwords or add a credential-bearing `QueryAffect`.

## Verification

Focused service integration tests cover follow-up requests and retries,
query/tenant/session isolation, old credentials after password changes,
access after user deletion, token removal with query cleanup, loss of node-local
state, explicit auth dispatch, and Arrow metadata. The token primitive has a
serialization/validation/redaction unit test.

```sh
cargo test -p databend-query --lib query_token
cargo test -p databend-query --test it servers::http::query_token
cargo test -p databend-query --test it servers::http::http_query_handlers
cargo clippy -p databend-query --lib --tests -- -D warnings
```

Multi-node routing and TLS/proxy deployments also require integration validation;
in-process endpoint tests alone do not establish those deployment properties.
