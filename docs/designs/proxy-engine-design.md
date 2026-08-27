# Proxy Engine Design

## Background

Databend FUSE tables rely on `CLUSTER BY`, statistics, Bloom indexes, and related pruning mechanisms during `Table::read_partitions(ctx, PushDownInfo, dry_run)`. A single physical table can only have one primary physical layout, so it is difficult for one Cluster Key to serve multiple high-cardinality blind lookup dimensions equally well.

Proxy Engine provides a declarative virtual table engine. Users query one logical proxy table, while Databend chooses the most suitable underlying physical table during the read path according to the query filters and the Cluster Keys of the configured target tables. This keeps multiple physical layouts hidden inside the database instead of exposing table-routing logic to application SQL or gateway code.

Trace, LLM observability, user activity logs, and similar workloads can all benefit from this capability. For example, in a trace workload, users may query by `trace_id` to inspect one request, or by `chat_id` to inspect one conversation context. Both are blind lookup dimensions, but one table cannot be physically clustered by both random IDs at the same time. Proxy Engine generalizes this into a multi-physical-layout routing problem.

## Goals And Non-goals

### Goals

- Provide a read-only `ENGINE = PROXY` table engine.
- Expose one unified schema through the proxy table.
- Bind multiple underlying physical target tables, each optimized for a different access pattern.
- Select a target table on the read path according to pushed-down query filters and target Cluster Keys.
- Keep business SQL transparent: `SELECT ... FROM proxy_table WHERE ...` does not need to rewrite table names.
- In the MVP, support `SELECT` only. Writes and data synchronization are handled by external dual-write, materialized views, or later Proxy Engine phases.

### Non-goals

- The MVP does not maintain data consistency among target tables.
- The MVP does not implement `INSERT`, `UPDATE`, `DELETE`, or `MERGE`.
- The MVP does not implement full CBO routing. It uses heuristic routing based on query filters and target table Cluster Keys.
- The MVP does not split `OR` predicates into multiple target scans followed by `UNION`.

## Databend Integration Points

The current Databend read path is roughly:

1. The SQL binder adds the table to planner metadata:
   `src/query/sql/src/planner/metadata/metadata.rs`
2. The optimizer writes filters into `Scan.push_down_predicates`:
   `src/query/sql/src/planner/optimizer/optimizers/rule/filter_rules/rule_push_down_filter_scan.rs`
3. `PhysicalPlanBuilder` converts `Scan.push_down_predicates` into `PushDownInfo.filters`:
   `src/query/service/src/physical_plans/physical_table_scan.rs`
4. `dyn Table::read_plan` calls `Table::read_partitions` for partition pruning:
   `src/query/sql/src/executor/table_read_plan.rs`
5. `TableScan::build_pipeline2` builds a table from `DataSourcePlan` and calls `read_data`:
   `src/query/service/src/physical_plans/physical_table_scan.rs`

Therefore, the least intrusive design is to implement a new `ProxyTable: Table`. The proxy table routes in `read_partitions` and `read_data` according to the match score between query filters and target table Cluster Keys, instead of rewriting table names at the AST layer.

## SQL Syntax

Suggested MVP syntax:

```sql
CREATE TABLE spans_proxy (
  start_time TIMESTAMP,
  trace_id STRING,
  chat_id STRING,
  span_id STRING,
  payload VARIANT
)
ENGINE = PROXY
OPTIONS (
  targets = 'spans_by_chat,spans_by_trace',
  default = 'spans_by_trace'
);
```

The target tables can use different physical layouts:

```sql
CREATE TABLE spans_by_chat (
  start_time TIMESTAMP,
  trace_id STRING,
  chat_id STRING,
  span_id STRING,
  payload VARIANT
)
ENGINE = FUSE
CLUSTER BY linear(to_yyyymmdd(start_time), chat_id);

CREATE TABLE spans_by_trace (
  start_time TIMESTAMP,
  trace_id STRING,
  chat_id STRING,
  span_id STRING,
  payload VARIANT
)
ENGINE = FUSE
CLUSTER BY linear(to_yyyymmdd(start_time), trace_id);
```

Options:

- `targets`: comma-separated underlying target tables. The MVP requires all targets to be in the same database as the proxy table.
- `default`: target table used when there is no match, the route cannot be determined, or multiple targets receive the same score.
- Users do not configure explicit routing rules. Proxy Engine reads `cluster_key_meta()` from each target table, analyzes the target Cluster Key expressions, and matches them against pushed-down query filters.
- Future versions may use JSON options for cross-database targets, weights, CBO strategy, or read consistency policy. The MVP should keep the declarative configuration minimal.

The current parser represents table engines as a fixed `Engine` enum, so it needs a new `Proxy` variant:

- `src/query/ast/src/ast/statements/table.rs`
- `src/query/ast/src/parser/statement.rs`

## Metadata And Create-time Validation

`ProxyTable` configuration is stored in the proxy table's own `TableMeta.options`:

- `targets`
- `default`
- optional `proxy_version`

Creating a proxy table must validate:

- All target tables exist.
- No target table is the proxy table itself.
- For the MVP, target tables should be restricted to `FUSE`.
- The proxy table schema is exactly the same as every target table schema.
- Field order, field names, data types, nullability, computed expressions, and column ID policy must be clarified. To reduce read-path risk, the MVP should require strict `TableSchema` equality, ideally with all target tables created from the same DDL.
- Each target table's Cluster Key must be parseable. Targets without Cluster Keys may exist, but they receive lower routing scores.
- `default` must be one of the configured `targets`.

Create-time validation can be added near `CreateTableInterpreter`:

- `src/query/service/src/interpreters/interpreter_table_create.rs`

## Storage Engine Registration

Add a new crate or module:

```text
src/query/storages/proxy/
```

Core structures:

```rust
pub struct ProxyTable {
    table_info: TableInfo,
    targets: Vec<ProxyTarget>,
    default_target: ProxyTarget,
}

pub struct ProxyTarget {
    catalog: String,
    database: String,
    table: String,
    cluster_keys: Vec<ClusterKeyExpr>,
}

pub struct ClusterKeyExpr {
    column: String,
    normalized_expr: String,
    position: usize,
}
```

Register the engine in `StorageFactory`:

```rust
creators.insert("PROXY".to_string(), Storage {
    creator: Arc::new(ProxyTable::try_create),
    descriptor: Arc::new(ProxyTable::description),
});
```

Registration point:

- `src/query/storages/factory/src/storage_factory.rs`

`SHOW ENGINES` should list the new engine. Proxy Engine does not own physical clustering, so `support_cluster_key` should be `false`.

## Automatic Routing Strategy

The MVP uses Cluster-Key-aware heuristic routing. Proxy Engine does not require users to declare mappings like `chat_id -> spans_by_chat` or `trace_id -> spans_by_trace`. Instead, it reads Cluster Keys from target table metadata and determines which target best matches the query filters.

Example:

- `spans_by_chat` has Cluster Key `linear(to_yyyymmdd(start_time), chat_id)`.
- `spans_by_trace` has Cluster Key `linear(to_yyyymmdd(start_time), trace_id)`.
- If the query filter contains `trace_id = '...'`, `spans_by_trace` receives a higher Cluster Key match score and is selected.
- If the query filter contains `chat_id = '...'`, `spans_by_chat` receives a higher Cluster Key match score and is selected.
- If the query filter only contains a `start_time` range, both targets match the time expression prefix, so the proxy uses `default`.

### Matching Model

The router should analyze `PushDownInfo.filters.filter: RemoteExpr<String>` rather than the AST. Databend already lowers pushable predicates into a storage-facing structure, and a `Table` implementation naturally sees `PushDownInfo`.

The router uses two kinds of information:

- Query-side constraints: extracted from `PushDownInfo.filters.filter`, including equality, `IN`, and range predicates.
- Target-side layouts: extracted from each target table's `cluster_key_meta()` / `resolve_cluster_keys()`, including Cluster Key columns and expression order.

The MVP can normalize Cluster Key expressions into recognized column sets. For example, `linear(to_yyyymmdd(start_time), trace_id)` becomes:

| position | expression | recognized columns |
|---|---|---|
| 0 | `to_yyyymmdd(start_time)` | `start_time` |
| 1 | `trace_id` | `trace_id` |

### Scoring Rules

| Query filter and Cluster Key match | Suggested score |
|---|---|
| Equality predicate on a high-selectivity non-prefix Cluster Key column, such as `trace_id = constant` | High |
| `IN` predicate on a high-selectivity non-prefix Cluster Key column | High |
| Range predicate on a Cluster Key prefix time expression, such as `start_time >= ... AND start_time < ...` | Medium |
| Generic range predicate only | Low |
| No Cluster Key column matched | 0 |
| Unsupported `OR` predicate | fallback |

When target scores differ, choose the highest-scoring target. When multiple targets tie, choose `default`. When all targets score 0, choose `default`.

### Fallback Rules

The MVP router only recognizes safe patterns:

- Recursively split `and_filters(...)`.
- Treat `eq(column, constant)` as an equality predicate.
- Treat `in(column, constant_list)` as a set equality predicate if the `RemoteExpr` shape is stable enough.
- Treat `gt`, `gte`, `lt`, and `lte` as range predicates.
- Fallback immediately on `or_filters(...)`.
- Fallback on function-wrapped columns, casts, expression columns, subqueries, and other complex shapes.

The router should return structured route results for `EXPLAIN` and query logs:

```rust
pub enum RouteReason {
    BestClusterKeyMatch {
        matched_columns: Vec<String>,
        score: u32,
    },
    TieBreakByDefault {
        candidates: Vec<String>,
    },
    FallbackNoPredicate,
    FallbackUnsupportedPredicate,
    FallbackOrPredicate,
}

pub struct RouteDecision {
    target: ProxyTarget,
    reason: RouteReason,
}
```

## Read Path Design

### `ProxyTable::read_partitions`

Flow:

1. Analyze filters from `PushDownInfo`.
2. Read or use cached Cluster Key information for target tables.
3. Score each target against the query filters.
4. Select the highest-scoring target. If the result is tied, unknown, or unmatched, select `default`.
5. Resolve the target table through `ctx.get_catalog(...).await?.get_table(...)`.
6. Call `target_table.read_partitions(ctx, push_downs, dry_run)`.
7. Return the target table's partitions and statistics.

Pseudo-code:

```rust
async fn read_partitions(
    &self,
    ctx: Arc<dyn TableContext>,
    push_downs: Option<PushDownInfo>,
    dry_run: bool,
) -> Result<(PartStatistics, Partitions)> {
    let decision = self.router.route(push_downs.as_ref())?;
    let target_table = self.resolve_target_table(ctx.clone(), &decision.target).await?;
    target_table.read_partitions(ctx, push_downs, dry_run).await
}
```

### `ProxyTable::read_data`

Flow:

1. Use the same deterministic routing decision derived from `plan.push_downs`.
2. Resolve the selected target table.
3. Build a delegated `DataSourcePlan`, preserving partitions, pushdowns, projections, internal columns, and other read metadata, while replacing `source_info` with the target table info.
4. Call `target_table.read_data(ctx, &delegated_plan, pipeline, put_cache)`.

It is not enough to let the default `dyn Table::read_plan` build a proxy `source_info` and then pass that plan to the target table. During execution, `build_table_from_source_plan` rebuilds the table from `source_info`:

- `src/query/service/src/sessions/query_ctx/table.rs`

Pseudo-code:

```rust
fn read_data(
    &self,
    ctx: Arc<dyn TableContext>,
    plan: &DataSourcePlan,
    pipeline: &mut Pipeline,
    put_cache: bool,
) -> Result<()> {
    let decision = self.router.route(plan.push_downs.as_ref())?;
    let target_table = block_on(self.resolve_target_table(ctx.clone(), &decision.target))?;

    let mut delegated_plan = plan.clone();
    delegated_plan.source_info = target_table.get_data_source_info();

    target_table.read_data(ctx, &delegated_plan, pipeline, put_cache)
}
```

`read_data` is currently synchronous, while resolving target tables may need async catalog access. Implementation options:

- Store the route target in `DataSourcePlan` or a `PushDownInfo` extension during `read_partitions`, then read the selected target from the plan during execution.
- Or maintain a synchronous target metadata cache in `ProxyTable`, with a clear refresh policy.

The first option is preferred: route during read planning and persist the target information in `DataSourcePlan`, so execution does not need another catalog lookup.

## DML And DDL Restrictions

MVP behavior:

- `INSERT INTO proxy_table`: return `Unsupported Operation: Proxy Engine is read-only in current version`.
- `UPDATE`, `DELETE`, and `MERGE`: return the same read-only error.
- `ALTER TABLE proxy_table`: the MVP should only allow Proxy options changes. Schema ALTER is not supported yet because it can leave target table schemas inconsistent.
- `DROP TABLE proxy_table`: drop only the proxy table metadata; do not drop target tables.
- `TRUNCATE proxy_table`: reject.

Read-write Proxy can be designed later with dual-write transactions, failure compensation, and target DDL synchronization.

## Consistency And Security Boundaries

The Proxy Engine MVP assumes target tables are kept eventually consistent by an external mechanism, such as:

- Application dual-write.
- Asynchronous materialized views.
- Background synchronization tasks.
- A future Databend write proxy.

Therefore, query consistency depends on the underlying synchronization mechanism. Short delays are usually acceptable for trace, log, and observability workloads. Strongly consistent business workloads should not use the MVP Proxy Engine as the only read entry point.

For privileges, the recommended policy is:

- Users need `SELECT` on the proxy table.
- Target tables are implementation details of the proxy table, so users do not need direct `SELECT` privileges on targets.
- Creating or modifying a proxy table requires sufficient privileges on all target tables.

## Observability

Expose the following information in `EXPLAIN` or query logs:

- proxy table name
- routed target
- route reason
- fallback reason
- matched Cluster Key columns and route score

Example:

```text
ProxyRoute: table=spans_proxy, target=spans_by_trace, reason=best_cluster_key_match, matched_columns=[trace_id], score=100
```

This is important for diagnosing why a query did or did not route to the expected target.

## Test Plan

### Unit Tests

- Options parsing.
- Schema validation.
- Cluster Key analyzer: parse expressions like `linear(to_yyyymmdd(start_time), trace_id)` and extract recognized columns.
- Filter analyzer: `eq`, `in`, range predicates, `and`, `or`, and fallback behavior.
- Route scorer: single-column hits, multi-column hits, and tied scores falling back to `default`.

### SQL Regression Tests

- `CREATE TABLE ... ENGINE = PROXY`.
- `SHOW CREATE TABLE`.
- `SHOW ENGINES`.
- `SELECT` returns data from target tables.
- Different filter conditions route to different targets according to target Cluster Keys, verified through `EXPLAIN` or mock statistics.
- `INSERT`, `UPDATE`, and `DELETE` return explicit read-only errors.

### Integration Tests

- Two FUSE target tables with different Cluster Keys.
- Proxy query `WHERE chat_id = ...` and `WHERE trace_id = ...` route to different targets.
- Time-only filters, indistinguishable filters, and unsupported filters fall back to the `default` target.

## Roadmap

### Phase 1: Read-only Cluster-key-aware Proxy Engine

Implement parser support, storage registration, metadata validation, Cluster Key analyzer, filter analyzer, route scorer, read routing, and `EXPLAIN` observability.

### Phase 2: DDL Coordination

Support proxy table schema ALTER and synchronize changes to all targets, with rollback or recoverable failure states.

### Phase 3: Write Proxy

Support INSERT dual-write and define sync/async behavior, consistency, failure compensation, and idempotency.

### Phase 4: Cost-based Routing

Use target table snapshots, block counts, pruning statistics, and estimated read bytes for CBO routing, so complex predicates no longer rely only on fixed heuristics.
