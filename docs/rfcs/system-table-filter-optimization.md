# RFC: System Table Query Optimization for Filtered Queries

## Summary

This RFC describes an optimization for querying system tables (`system.tables`, `system.databases`, `system.columns`) when specific filters are applied. The optimization avoids loading all ownership records from meta storage when the query filters specify a small number of specific objects (≤20).

## Motivation

### Problem Statement

When users query system tables with specific filters like:

```sql
SELECT * FROM system.tables WHERE database = 'mydb' AND name = 'mytable';
SELECT * FROM system.databases WHERE name = 'mydb';
SELECT * FROM system.columns WHERE database = 'mydb' AND table = 'mytable';
```

The current implementation has a "fast path" that avoids listing all tables/databases. However, it still calls `get_visibility_checker()` which internally calls `list_ownerships()` - loading **ALL** ownership records from meta storage.

In deployments with millions of tables, this becomes a significant performance bottleneck:
- Loading millions of ownership records is expensive
- Network latency to meta service multiplied by data size
- Memory pressure from deserializing large ownership lists

### Real-world Impact

For a deployment with 1 million tables:
- Current behavior: Load ~1M ownership records even when querying a single table
- After optimization: Load only 1-20 specific ownership records

## Design

### Core Principle

Trade N additional meta service calls for avoiding loading millions of records. When the filter specifies ≤20 specific objects, this trade-off is highly favorable.

### Optimization Conditions

The optimized path is used when ALL of the following conditions are met:

1. **Filter count ≤ 20**: The number of filtered objects (databases × tables) is small
2. **Not WITH_HISTORY**: History queries still use the full path
3. **Not external catalog**: External catalogs don't require permission checks

### Permission Check Strategy

Instead of loading all ownerships upfront:

1. For each specific object in the filter:
   - Call `get_ownership()` to get the single ownership record
   - Check if the current user is the owner (via effective roles)
   - If not owner, use lightweight grant-based visibility check

2. The lightweight check function directly iterates user/role grants without needing the full ownership list

### New Helper Functions

Two new public functions in `visibility_checker.rs`:

```rust
/// Check table visibility without loading all ownerships
pub fn check_table_visibility_with_roles(
    user: &UserInfo,
    roles: &[RoleInfo],
    catalog: &str,
    db_name: &str,
    db_id: u64,
    table_id: u64,
) -> bool

/// Check database visibility without loading all ownerships
pub fn check_database_visibility_with_roles(
    user: &UserInfo,
    roles: &[RoleInfo],
    catalog: &str,
    db_name: &str,
    db_id: u64,
) -> bool
```

These functions check visibility by iterating through user and role grants directly, checking for:
- Global privileges
- Database-level privileges (by ID or name)
- Table-level privileges (by ID)
- System databases (always visible)

## Implementation

### Files Modified

1. **`src/query/users/src/visibility_checker.rs`**
   - Added `check_table_visibility_with_roles()`
   - Added `check_database_visibility_with_roles()`

2. **`src/query/users/src/lib.rs`**
   - Exported the new helper functions

3. **`src/query/storages/system/src/tables_table.rs`**
   - Added optimized path in `get_full_data_from_catalogs()` (lines 465-567)
   - Threshold constant: `OPTIMIZED_PATH_THRESHOLD = 20`

4. **`src/query/storages/system/src/databases_table.rs`**
   - Added optimized path in `get_full_data()` (lines 151-209)
   - Threshold constant: `OPTIMIZED_PATH_THRESHOLD = 20`

5. **`src/query/storages/system/src/columns_table.rs`**
   - Added optimized path in `dump_tables()` (lines 296-383)
   - Threshold constant: `OPTIMIZED_PATH_THRESHOLD = 20`

### Code Flow

```
Query: SELECT * FROM system.tables WHERE database='db1' AND name='t1'

┌─────────────────────────────────────────────────────────────┐
│                    Filter Extraction                         │
│  Extract: db_name=['db1'], tables_names=['t1']              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Optimization Check                              │
│  filter_count = 1 * 1 = 1                                   │
│  use_optimized_path = (1 <= 20) && !WITH_HISTORY            │
│                       && !is_external_catalog               │
│  Result: true → Use optimized path                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Optimized Path                                  │
│  1. Get current_user and effective_roles (cached)           │
│  2. For each (db, table) in filter:                         │
│     a. get_table() → get table info                         │
│     b. get_ownership() → get single ownership               │
│     c. Check if user is owner via roles                     │
│     d. If not owner: check_table_visibility_with_roles()    │
│     e. If visible: add to result                            │
└─────────────────────────────────────────────────────────────┘
```

### Comparison: Old vs New

| Aspect | Old Behavior | New Behavior (Optimized) |
|--------|--------------|-------------------------|
| Meta calls for ownership | 1 call returning millions of records | N calls (≤20) returning 1 record each |
| Memory usage | High (deserialize all ownerships) | Low (only needed ownerships) |
| Network transfer | Large payload | Small payloads |
| Latency | Dominated by list_ownerships | Dominated by get_table |

## Threshold Selection

The threshold of 20 was chosen based on:

1. **Break-even analysis**: At 20 objects, the overhead of N individual calls is still much less than loading millions of records
2. **Common use cases**: Most filtered queries target 1-5 specific objects
3. **Safety margin**: Even at 20 objects, the optimization provides significant benefit

## External Catalogs

External catalogs (Iceberg, Hive, etc.) are explicitly excluded from this optimization because:
- They don't use Databend's permission system
- No ownership checks are needed
- The visibility checker is already `None` for external catalogs

## Backward Compatibility

This change is fully backward compatible:
- No API changes
- No behavior changes for queries exceeding the threshold
- Same permission semantics, just faster execution path

## Testing

The optimization can be verified by:

1. **Performance testing**: Query system tables with filters in a deployment with many tables
2. **Correctness testing**: Verify same results as before for various permission scenarios
3. **Edge cases**:
   - Exactly 20 objects (uses optimized path)
   - 21 objects (uses slow path)
   - Mixed ID and name filters
   - External catalogs (always slow path, no permission check)

## Future Work

1. **Configurable threshold**: Allow users to tune the threshold via settings
2. **Metrics**: Add metrics to track optimization hit rate
3. **Extended optimization**: Apply similar pattern to other system tables
