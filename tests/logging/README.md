# Data for logging tests

- setup a vector server with OTLP source

- setup a query server with OTLP logging config

- run queries and check logs

## Connection and Sequence access history

With `access_history` enabled, successful named Connection resolution and Sequence
value allocation are logged in `direct_objects_accessed`, including the user and
current role. Successful access is recorded in both compatibility and strict
privilege modes. `DEFAULT nextval(...)` is included only when a value is allocated.
Named `DESC` and successful `DROP` operations are also recorded. The log stores
object names, not storage credentials or generated values. It records the access
itself, not the final status of the statement; use `query_id` to correlate with
`system_history.query_history`.

Before enabling the global privilege checks, collect representative workload
history and compare these object/user/role combinations with existing grants.
The current role is attribution, not proof of which grant authorized the access.
Using a stage or table whose storage parameters were already resolved does not
imply a new Connection lookup. Reading a share as a consumer likewise does not
require a grant on the provider's Connection.

Named accesses are separate from the table/stage records for the same query. Each
node records an object once per query; use `DISTINCT` when inspecting accesses from
multiple nodes:

```sql
SELECT DISTINCT h.query_id, h.user_name,
       a.value['role_name']::STRING AS role_name,
       a.value['object_domain']::STRING AS object_type,
       a.value['object_name']::STRING AS object_name
FROM system_history.access_history AS h,
     LATERAL FLATTEN(input => h.direct_objects_accessed) AS a
WHERE a.value['object_domain']::STRING IN ('Connection', 'Sequence');
```

Run `bash tests/logging/history_table/check_named_object_access.sh` against a test
server with `history_table/history_table.toml` enabled to verify the persisted records.
