# Removal of the DML retry label

Databend no longer supports `deduplicate_label`. The SQL setting has been
removed. The HTTP `X-DATABEND-DEDUPLICATE-LABEL` header is ignored and does
not make a write safe to retry. Query does not read or write retry-label
markers in Meta. Existing markers expire without migration.

Remove the label from SQL `SET`, `SETTINGS`, and `SET_VAR`, HTTP session
settings, and request headers. A direct `SET deduplicate_label` fails
with `UnknownVariable`. Do not rely on the absence of an error: entry
points that ignore unknown settings no longer apply the label either.

Before upgrading, stop automatic retries that depend on the label and
reconcile in-flight writes against the destination. After upgrading, use
an application-level idempotency strategy before retrying an uncertain
write. Resending an old label cannot prevent a duplicate.

File-level deduplication for `COPY INTO <table>` and Databend's internal
transaction retry mechanisms are unchanged.
