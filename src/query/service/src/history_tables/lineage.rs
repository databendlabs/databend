// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(super) const LINEAGE_UNRESOLVED_TABLE: &str = "lineage_unresolved";

// Source and target always retain data-flow orientation. Directional views only change which
// endpoint is matched by the current frontier and which endpoint becomes the next frontier.
pub(super) const CREATE_LINEAGE_VIEWS: &[&str] = &[
    r#"CREATE OR REPLACE VIEW system_history.lineage AS
WITH current_objects AS (
    SELECT 'TABLE' AS object_type, catalog, database, name, table_id
    FROM system.tables
    WHERE dropped_on IS NULL
    UNION ALL
    SELECT 'VIEW' AS object_type, catalog, database, name, table_id
    FROM system.views
    WHERE dropped_on IS NULL
    UNION ALL
    SELECT 'STAGE' AS object_type, '' AS catalog, '' AS database, name, NULL::UInt64 AS table_id
    FROM system.stages
)
SELECT
    lh.*,
    iff(coalesce(lh.source_catalog_type, '') = 'DEFAULT', s.table_id, lh.source_id) AS source_resolved_id,
    iff(coalesce(lh.source_catalog_type, '') = 'DEFAULT', s.catalog, lh.source_catalog) AS source_resolved_catalog,
    iff(coalesce(lh.source_catalog_type, '') = 'DEFAULT', s.database, lh.source_database) AS source_resolved_database,
    iff(coalesce(lh.source_catalog_type, '') = 'DEFAULT', s.name, lh.source_name) AS source_resolved_name,
    iff(coalesce(lh.target_catalog_type, '') = 'DEFAULT', t.table_id, lh.target_id) AS target_resolved_id,
    iff(coalesce(lh.target_catalog_type, '') = 'DEFAULT', t.catalog, lh.target_catalog) AS target_resolved_catalog,
    iff(coalesce(lh.target_catalog_type, '') = 'DEFAULT', t.database, lh.target_database) AS target_resolved_database,
    iff(coalesce(lh.target_catalog_type, '') = 'DEFAULT', t.name, lh.target_name) AS target_resolved_name,
    iff(coalesce(lh.source_catalog_type, '') = 'DEFAULT', iff(s.table_id IS NULL, concat('STAGE::NAME::', s.name), iff(lh.source_address_kind = 'NAME', concat(s.object_type, '::NAME::', s.catalog, '.', s.database, '.', s.name), concat(s.object_type, '::ID::', to_string(s.table_id)))), lh.source_lineage_key) AS source_object_key,
    iff(coalesce(lh.target_catalog_type, '') = 'DEFAULT', iff(t.table_id IS NULL, concat('STAGE::NAME::', t.name), iff(lh.target_address_kind = 'NAME', concat(t.object_type, '::NAME::', t.catalog, '.', t.database, '.', t.name), concat(t.object_type, '::ID::', to_string(t.table_id)))), lh.target_lineage_key) AS target_object_key
FROM system_history.lineage_unresolved AS lh
LEFT JOIN current_objects AS s
    ON coalesce(lh.source_catalog_type, '') = 'DEFAULT'
    AND lh.source_object_type = s.object_type
    AND (
        (lh.source_address_kind = 'ID' AND lh.source_id = s.table_id)
        OR (lh.source_address_kind = 'NAME' AND lh.source_catalog = s.catalog AND lh.source_database = s.database AND lh.source_name = s.name)
    )
LEFT JOIN current_objects AS t
    ON coalesce(lh.target_catalog_type, '') = 'DEFAULT'
    AND lh.target_object_type = t.object_type
    AND (
        (lh.target_address_kind = 'ID' AND lh.target_id = t.table_id)
        OR (lh.target_address_kind = 'NAME' AND lh.target_catalog = t.catalog AND lh.target_database = t.database AND lh.target_name = t.name)
    )
WHERE (coalesce(lh.source_catalog_type, '') != 'DEFAULT' OR s.name IS NOT NULL)
  AND (coalesce(lh.target_catalog_type, '') != 'DEFAULT' OR t.name IS NOT NULL)"#,
    r#"CREATE OR REPLACE VIEW system_history.lineage_by_target AS
SELECT
    query_id,
    event_time,
    query_kind,
    lineage_kind,
    source_catalog_type,
    target_catalog_type,
    column_lineage_hash,
    concat(source_object_key, '->', target_object_key) AS edge_key,
    target_lineage_key AS match_key,
    target_object_key AS current_object_key,
    source_object_key AS next_object_key,
    source_catalog_type AS next_catalog_type,
    [
        concat(source_object_type, '::ID::', to_string(source_resolved_id)),
        iff(source_object_type = 'STAGE', concat('STAGE::NAME::', source_resolved_name), concat(source_object_type, '::NAME::', source_resolved_catalog, '.', source_resolved_database, '.', source_resolved_name))
    ] AS next_lookup_keys,
    iff(target_object_type = 'VIEW', 'NAME', target_address_kind) AS current_column_address_kind,
    iff(source_object_type = 'VIEW', 'NAME', source_address_kind) AS next_column_address_kind,
    source_resolved_catalog AS next_object_catalog,
    source_resolved_database AS next_object_database,
    source_resolved_name AS next_object_short_name,
    source_object_type,
    iff(source_object_type = 'STAGE', source_resolved_name, concat(source_resolved_catalog, '.', source_resolved_database, '.', source_resolved_name)) AS source_object_name,
    source_resolved_id AS source_object_id,
    source_resolved_catalog AS source_object_catalog,
    source_resolved_database AS source_object_database,
    source_resolved_name AS source_object_short_name,
    target_object_type,
    iff(target_object_type = 'STAGE', target_resolved_name, concat(target_resolved_catalog, '.', target_resolved_database, '.', target_resolved_name)) AS target_object_name,
    target_resolved_id AS target_object_id,
    target_resolved_catalog AS target_object_catalog,
    target_resolved_database AS target_object_database,
    target_resolved_name AS target_object_short_name,
    target_to_source_columns AS column_map
FROM system_history.lineage"#,
    r#"CREATE OR REPLACE VIEW system_history.lineage_by_source AS
SELECT
    query_id,
    event_time,
    query_kind,
    lineage_kind,
    source_catalog_type,
    target_catalog_type,
    column_lineage_hash,
    concat(source_object_key, '->', target_object_key) AS edge_key,
    source_lineage_key AS match_key,
    source_object_key AS current_object_key,
    target_object_key AS next_object_key,
    target_catalog_type AS next_catalog_type,
    [
        concat(target_object_type, '::ID::', to_string(target_resolved_id)),
        iff(target_object_type = 'STAGE', concat('STAGE::NAME::', target_resolved_name), concat(target_object_type, '::NAME::', target_resolved_catalog, '.', target_resolved_database, '.', target_resolved_name))
    ] AS next_lookup_keys,
    iff(source_object_type = 'VIEW', 'NAME', source_address_kind) AS current_column_address_kind,
    iff(target_object_type = 'VIEW', 'NAME', target_address_kind) AS next_column_address_kind,
    target_resolved_catalog AS next_object_catalog,
    target_resolved_database AS next_object_database,
    target_resolved_name AS next_object_short_name,
    source_object_type,
    iff(source_object_type = 'STAGE', source_resolved_name, concat(source_resolved_catalog, '.', source_resolved_database, '.', source_resolved_name)) AS source_object_name,
    source_resolved_id AS source_object_id,
    source_resolved_catalog AS source_object_catalog,
    source_resolved_database AS source_object_database,
    source_resolved_name AS source_object_short_name,
    target_object_type,
    iff(target_object_type = 'STAGE', target_resolved_name, concat(target_resolved_catalog, '.', target_resolved_database, '.', target_resolved_name)) AS target_object_name,
    target_resolved_id AS target_object_id,
    target_resolved_catalog AS target_object_catalog,
    target_resolved_database AS target_object_database,
    target_resolved_name AS target_object_short_name,
    source_to_target_columns AS column_map
FROM system_history.lineage"#,
];

// Keep drops in reverse dependency order as directional lineage views are added.
pub(super) const DROP_LINEAGE_VIEWS: &[&str] = &[
    "DROP VIEW IF EXISTS system_history.lineage_by_source",
    "DROP VIEW IF EXISTS system_history.lineage_by_target",
    "DROP VIEW IF EXISTS system_history.lineage",
];

#[cfg(test)]
mod tests {
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;

    use super::*;

    #[test]
    fn test_lineage_view_sql_parses() {
        for sql in CREATE_LINEAGE_VIEWS.iter().chain(DROP_LINEAGE_VIEWS) {
            let tokens = tokenize_sql(sql).unwrap();
            parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
        }
        assert!(CREATE_LINEAGE_VIEWS[0].contains("source_catalog_type, '') != 'DEFAULT'"));
        assert!(CREATE_LINEAGE_VIEWS[0].contains("target_catalog_type, '') != 'DEFAULT'"));
    }
}
