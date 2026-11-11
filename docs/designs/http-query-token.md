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
with a client session, that session's cookie or session headers. In management
mode, an explicit tenant header must match the authenticated owner's tenant.
Outside management mode, tenant headers follow the existing session validation
rules: headers ignored by Basic authentication in sandbox-enabled configurations
are also ignored with query tokens. When omitted, the validated query supplies
the authenticated tenant. Capability validation uses the query ID from the
route, not the caller's `X-DATABEND-QUERY-ID` header.

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

Each query records its **owner tenant** from a snapshot taken by HTTP middleware
after authentication and before applying request-body session settings. This is
not the execution tenant: `session.settings.sandbox_tenant` may change where SQL
executes, but must not change who owns the query. Follow-ups have no body, and
submission retries compare against the same immutable authenticated tenant.

Every handler checks the actual query object's owner tenant, user and client
session before exposing results or its token. Query-token authentication is
revalidated against that object; final/kill perform this check while holding the query map
lock, so a reused query ID cannot let a request authenticated for an old query
close a replacement query.

An authenticated retry of query submission with the same query ID can return
the existing query and token only for the same tenant, user and client session.
This does not add automatic SQL replay or guarantee recovery if the query record
has already disappeared.

## Error and compatibility behavior

- A missing local query returns HTTP 404, including expired/removed query state,
  node restarts and a request sent to a node without that query. This check cannot
  validate a capability after its backing state has disappeared. A malformed or
  wrong token for an existing query returns HTTP 401. This intentionally exposes
  whether the supplied query ID exists, so clients can distinguish missing state
  from invalid credentials and avoid session refresh loops.
- Keep sticky-node and expected-node headers. For a locally retained query, the
  existing expected-node check still produces the detailed routing error. If the
  query is absent locally, capability authentication returns the query-not-found
  404 before the handler's detailed expected-node check; this is not resumable
  state or a reason to refresh session credentials.
- Ownership checks apply to **all** authentication methods, not just query
  tokens. State now checks ownership; final/kill now check user and owner tenant
  as well as client session. Cross-user or cross-tenant operations formerly
  accepted by these endpoints are now rejected. Sandbox execution settings do
  not alter the authenticated owner, so Basic/session-token follow-ups continue
  to work without sending the settings again.
- Multiple `X-DATABEND-AUTH-METHOD` headers are rejected on authenticated
  endpoints, rather than accepting the first value.
- Submission responses, pages, submission/page retries and the legacy final
  response include `query_token`. Retaining it on these authorized responses
  supports response recovery but expands the number of responses carrying the
  capability. State responses and successful kill responses do not include it.

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
state, missing-query 404 versus wrong-token 401, routing diagnostics, explicit
auth dispatch, and Arrow metadata. Sandbox compatibility tests also cover Basic
and native session-token state/page/final/kill requests and submission retries,
including session settings returned by `SET sandbox_tenant`. The token primitive
has a serialization/validation/redaction unit test.

```sh
cargo test -p databend-query --lib query_token
cargo test -p databend-query --test it servers::http::query_token
cargo test -p databend-query --test it servers::http::http_query_handlers
cargo clippy -p databend-query --lib --tests -- -D warnings
```

Multi-node routing and TLS/proxy deployments also require integration validation;
in-process endpoint tests alone do not establish those deployment properties.
