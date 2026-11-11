# SQL KILL identifiers and authorization

`KILL QUERY '<id>'` accepts a node-local MySQL connection id, a session id, or a
query id. `KILL CONNECTION '<id>'` accepts connection and session ids only; if the
identifier resolves to a query, it returns `BadArguments` without disconnecting
that query's session.

## Identifiers

New internal session ids have the form `p-<UUID>`, where `p` refers to the SQL
process in `system.processes`, not an operating-system PID. Both the SQL
`connection_id()` function and `system.processes.id` expose this prefixed string;
MySQL's numeric protocol connection id is unchanged. HTTP login/client-session
ids are separate and retain their existing format. Temporary-table session keys
and logs treat internal session ids as opaque strings.

`p-` is reserved: an `X-DATABEND-QUERY-ID` beginning with this prefix is
rejected with HTTP 400. Existing clients may continue using 32-character hex ids,
UUIDs, UUID-plus-sequence ids, or other non-reserved query ids.

SQL KILL resolves its argument in this order:

1. If it parses as `u32`, use the MySQL connection id on the receiving node. This
   preserves MySQL client Ctrl+C. Query ids that parse as numeric connection ids
   cannot be addressed by query id with SQL KILL.
2. If it starts with `p-`, look up that session id.
3. Otherwise, look up an active coordinator session's current query id, then
   fall back to an unprefixed legacy session id. The fallback supports sessions
   on nodes that have not yet been upgraded. A UUID alone is inherently
   ambiguous during this compatibility period; a query-id match takes priority.

Query-id lookup includes queued queries, but excludes FlightRPC fragment sessions:
these are not registered in the active client-session map. Requests to other
warehouse nodes therefore locate the coordinator rather than canceling an
arbitrary distributed fragment.

## Authorization and cancellation

A user may cancel their own queries or connections without `SUPER`. Ownership
requires the same tenant and the complete user identity (username and hostname).
Other targets, including sessions with no authenticated user, require global
`SUPER`; role grants are included. Authorization precedes queue removal and
cancellation. A denied target returns `PermissionDenied` (1063); a missing target
returns `UnknownSession` (1053). This distinction makes permission failures more
actionable than a misleading not-found error. Session and query ids are already
visible through `system.processes` where callers can access those rows, but this
change also gives callers a target-existence signal outside that visibility.
It deliberately replaces the previous existence-hiding error contract; it does
not relax the ownership or SUPER checks.

Query-id cancellation checks and pins the matching query context under the
session's context-slot lock. Queue removal happens before releasing that lock,
and only the pinned context is canceled. A connection attaching its next query
cannot cause that new context to be canceled by the old query's id.

SUPER requesters retain the legacy Flight kill action. Other requesters use only
`/actions/kill_query_v2`, with their tenant, user identity, and SUPER status. An
old node that does not support v2 is treated as not finding the target; there is
no fallback to the unrestricted legacy action. Old receivers interpret a query
id as a session id, so query-id KILL against a coordinator on an old node is not
supported during rolling upgrades. Its actual session id remains usable, with
the pre-upgrade authorization requirements.

## HTTP query lifecycle

SQL KILL does not call or change the HTTP query-kill endpoint. After execution
stops, `/page` and `/final` return the normal HTTP 200 query-response envelope
with `state: Failed` and `AbortedQuery` (1043). Running queries canceled by query
id include `Query canceled by KILL QUERY`; queued queries may instead report
that they were killed while queued. Buffered result rows do not turn the failed
query into success. The query-log event is classified as `Aborted`.

The normal HTTP lifecycle remains in effect: clients should finalize results;
finalization decrements the active-query count and the manager removes the
retained record after its existing retention timeout. SQL KILL does not bypass
this cleanup or change the HTTP `/kill` endpoint's distinct closed-query response.
