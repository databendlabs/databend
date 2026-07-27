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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::SystemTime;

use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextQueryInfo;
use databend_common_config::GlobalConfig;
use databend_common_meta_app::schema::CatalogType;
use databend_common_sql::QueryLineage;
use databend_common_sql::QueryLineageColumn;
use databend_common_sql::QueryLineageKind;
use databend_common_sql::QueryLineageRelation;
use databend_common_sql::QueryLineageRelationKind;
use log::info;
use log::warn;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::sessions::QueryContext;
use crate::sessions::convert_query_log_timestamp;

const LINEAGE_LOG_TARGET: &str = "databend::log::lineage";

#[derive(Serialize)]
struct LineageLogEntry {
    operation: &'static str,
    query_id: String,
    event_time: i64,
    query_kind: String,
    lineage_kind: &'static str,
    source: LineageEndpoint,
    target: LineageEndpoint,
    column_lineage_hash: String,
    source_to_target_columns: BTreeMap<String, Vec<String>>,
    target_to_source_columns: BTreeMap<String, Vec<String>>,
}

#[derive(Serialize)]
struct LineageObjectDeleteLogEntry {
    operation: &'static str,
    object_id: u64,
    query_id: String,
    event_time: i64,
    query_kind: String,
}

#[derive(Clone, Serialize)]
struct LineageEndpoint {
    object_type: &'static str,
    address_kind: &'static str,
    catalog_type: Option<&'static str>,
    catalog: String,
    database: String,
    name: String,
    id: Option<u64>,
    lineage_key: String,
}

pub fn log_query_lineage(ctx: &Arc<QueryContext>) {
    let Some(lineage) = ctx.get_query_lineage() else {
        return;
    };

    let query_id = ctx.get_id();
    let query_kind = ctx.get_query_kind().to_string();
    let event_time = convert_query_log_timestamp(SystemTime::now());

    for entry in build_log_entries(lineage, query_id.clone(), query_kind.clone(), event_time) {
        match serde_json::to_string(&entry) {
            Ok(json) => info!(target: LINEAGE_LOG_TARGET, "{}", json),
            Err(err) => warn!("Failed to serialize query lineage log: {:?}", err),
        }
    }
}

/// Record an id-addressed object tombstone for asynchronous lineage cleanup.
/// The history transform is replay-safe, so this log must not affect the DDL result.
pub fn log_lineage_object_deletion(ctx: &Arc<QueryContext>, object_id: u64) {
    if !lineage_enabled() {
        return;
    }

    let entry = LineageObjectDeleteLogEntry {
        operation: "DELETE_OBJECT",
        object_id,
        query_id: ctx.get_id(),
        event_time: convert_query_log_timestamp(SystemTime::now()),
        query_kind: ctx.get_query_kind().to_string(),
    };
    match serde_json::to_string(&entry) {
        Ok(json) => info!(target: LINEAGE_LOG_TARGET, "{}", json),
        Err(err) => warn!("Failed to serialize lineage deletion log: {:?}", err),
    }
}

fn lineage_enabled() -> bool {
    GlobalConfig::instance()
        .log
        .history
        .is_table_enabled("lineage_unresolved")
}

fn build_log_entries(
    lineage: QueryLineage,
    query_id: String,
    query_kind: String,
    event_time: i64,
) -> Vec<LineageLogEntry> {
    let lineage_kind = lineage_kind_name(lineage.kind);
    let mut entries = Vec::new();

    for lineage_target in lineage.targets {
        let target = endpoint_from_relation(lineage.kind, false, &lineage_target.relation);
        for lineage_source in lineage_target.sources {
            let source = endpoint_from_relation(lineage.kind, true, &lineage_source.relation);
            if source.lineage_key == target.lineage_key {
                continue;
            }

            // Stages are object-level lineage endpoints. Their file fields are not stable schema
            // columns, so COPY edges intentionally carry empty column maps.
            let column_pairs = if source.object_type == "STAGE" || target.object_type == "STAGE" {
                BTreeSet::new()
            } else {
                lineage_source
                    .columns
                    .into_iter()
                    .map(|edge| {
                        (
                            column_address(&source, edge.source),
                            column_address(&target, edge.target),
                        )
                    })
                    .collect::<BTreeSet<_>>()
            };
            let (source_to_target_columns, target_to_source_columns) = column_maps(&column_pairs);

            entries.push(LineageLogEntry {
                operation: "UPSERT_EDGE",
                query_id: query_id.clone(),
                event_time,
                query_kind: query_kind.clone(),
                lineage_kind,
                source: source.clone(),
                target: target.clone(),
                column_lineage_hash: column_lineage_hash(&column_pairs),
                source_to_target_columns,
                target_to_source_columns,
            });
        }
    }

    entries
}

fn lineage_kind_name(kind: QueryLineageKind) -> &'static str {
    match kind {
        QueryLineageKind::Ctas => "CTAS",
        QueryLineageKind::Dml => "DML",
        QueryLineageKind::CreateView => "CREATE_VIEW",
    }
}

fn object_type_name(kind: &QueryLineageRelationKind) -> &'static str {
    match kind {
        QueryLineageRelationKind::Table => "TABLE",
        QueryLineageRelationKind::View => "VIEW",
        QueryLineageRelationKind::Stage => "STAGE",
    }
}

fn catalog_type_name(catalog_type: Option<CatalogType>) -> Option<&'static str> {
    catalog_type.map(|catalog_type| match catalog_type {
        CatalogType::Default => "DEFAULT",
        CatalogType::Hive => "HIVE",
        CatalogType::Iceberg => "ICEBERG",
        CatalogType::Paimon => "PAIMON",
    })
}

fn endpoint_from_relation(
    kind: QueryLineageKind,
    is_source: bool,
    relation: &QueryLineageRelation,
) -> LineageEndpoint {
    let object_type = object_type_name(&relation.kind);
    // View definitions retain source names. Renaming a referenced object should therefore make
    // the edge inactive until that name resolves again. Other endpoints prefer stable IDs.
    let relation_id = if relation.catalog_type != Some(CatalogType::Default)
        || kind == QueryLineageKind::CreateView && is_source
    {
        None
    } else {
        relation.id
    };
    let (address_kind, address_value) = match relation_id {
        Some(id) => ("ID", id.to_string()),
        None if relation.kind == QueryLineageRelationKind::Stage => ("NAME", relation.name.clone()),
        None => (
            "NAME",
            format!(
                "{}.{}.{}",
                relation.catalog, relation.database, relation.name
            ),
        ),
    };
    let lineage_key = format!("{object_type}::{address_kind}::{address_value}");

    LineageEndpoint {
        object_type,
        address_kind,
        catalog_type: catalog_type_name(relation.catalog_type),
        catalog: relation.catalog.clone(),
        database: relation.database.clone(),
        name: relation.name.clone(),
        id: relation_id,
        lineage_key,
    }
}

fn column_address(endpoint: &LineageEndpoint, column: QueryLineageColumn) -> String {
    // View output column ids are query-plan ordinals rather than stable schema ids. Keep view
    // columns name-addressed even though the view object itself has a stable table id.
    if endpoint.object_type == "VIEW" || endpoint.address_kind == "NAME" {
        column.name
    } else {
        column.id.to_string()
    }
}

fn column_maps(
    column_pairs: &BTreeSet<(String, String)>,
) -> (BTreeMap<String, Vec<String>>, BTreeMap<String, Vec<String>>) {
    let mut source_to_target = BTreeMap::<String, BTreeSet<String>>::new();
    let mut target_to_source = BTreeMap::<String, BTreeSet<String>>::new();
    for (source, target) in column_pairs {
        source_to_target
            .entry(source.clone())
            .or_default()
            .insert(target.clone());
        target_to_source
            .entry(target.clone())
            .or_default()
            .insert(source.clone());
    }

    let into_map = |map: BTreeMap<String, BTreeSet<String>>| {
        map.into_iter()
            .map(|(key, values)| (key, values.into_iter().collect()))
            .collect()
    };
    (into_map(source_to_target), into_map(target_to_source))
}

fn column_lineage_hash(column_pairs: &BTreeSet<(String, String)>) -> String {
    let mut hasher = Sha256::new();
    for (source, target) in column_pairs {
        for value in [source, target] {
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use databend_common_sql::LineageSource;
    use databend_common_sql::LineageTarget;
    use databend_common_sql::QueryLineageColumnEdge;

    use super::*;

    #[test]
    fn test_build_log_entries_uses_bidirectional_column_maps() {
        let captured = lineage(
            QueryLineageKind::Dml,
            relation("source", Some(10)),
            relation("target", Some(20)),
            vec![
                edge(1, "a", 3, "x"),
                edge(2, "b", 3, "x"),
                edge(1, "a", 3, "x"),
            ],
        );

        let entries = build_log_entries(captured, "query-1".into(), "INSERT".into(), 1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, "UPSERT_EDGE");
        assert_eq!(
            entries[0].source_to_target_columns,
            BTreeMap::from([
                ("1".to_string(), vec!["3".to_string()]),
                ("2".to_string(), vec!["3".to_string()]),
            ])
        );
        assert_eq!(
            entries[0].target_to_source_columns,
            BTreeMap::from([("3".to_string(), vec!["1".to_string(), "2".to_string()],)])
        );

        let reordered = lineage(
            QueryLineageKind::Dml,
            relation("source", Some(10)),
            relation("target", Some(20)),
            vec![edge(2, "b", 3, "x"), edge(1, "a", 3, "x")],
        );
        let reordered = build_log_entries(reordered, "query-2".into(), "INSERT".into(), 2);
        assert_eq!(
            entries[0].column_lineage_hash,
            reordered[0].column_lineage_hash
        );
    }

    #[test]
    fn test_create_view_uses_source_names_and_target_ids() {
        let lineage = lineage(
            QueryLineageKind::CreateView,
            relation("source", Some(10)),
            view_relation("view", Some(20)),
            vec![edge(1, "a", 3, "x")],
        );

        let entries = build_log_entries(lineage, "query-1".into(), "CREATE_VIEW".into(), 1);
        assert_eq!(entries[0].source.address_kind, "NAME");
        assert_eq!(entries[0].target.address_kind, "ID");
        assert_eq!(
            entries[0].source_to_target_columns,
            BTreeMap::from([("a".to_string(), vec!["x".to_string()],)])
        );
        assert_eq!(
            entries[0].target_to_source_columns,
            BTreeMap::from([("x".to_string(), vec!["a".to_string()],)])
        );
    }

    #[test]
    fn test_build_log_entries_skips_self_loop() {
        let same = relation("table", Some(10));
        let lineage = lineage(QueryLineageKind::Dml, same.clone(), same, vec![edge(
            1, "a", 1, "a",
        )]);
        assert!(build_log_entries(lineage, "query-1".into(), "INSERT".into(), 1).is_empty());
    }

    #[test]
    fn test_copy_stage_uses_name_addressing_without_column_maps() {
        let lineage = lineage(
            QueryLineageKind::Dml,
            relation_with_kind("named_stage", None, QueryLineageRelationKind::Stage),
            relation("target", Some(20)),
            vec![edge(1, "file_col", 3, "x")],
        );

        let entries = build_log_entries(lineage, "query-1".into(), "COPY".into(), 1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].source.object_type, "STAGE");
        assert_eq!(entries[0].source.address_kind, "NAME");
        assert_eq!(entries[0].source.catalog_type, None);
        assert_eq!(entries[0].source.lineage_key, "STAGE::NAME::named_stage");
        assert!(entries[0].source_to_target_columns.is_empty());
        assert!(entries[0].target_to_source_columns.is_empty());
    }

    #[test]
    fn test_external_catalog_uses_captured_names() {
        let source = QueryLineageRelation {
            catalog: "iceberg_catalog".to_string(),
            database: "db".to_string(),
            name: "source".to_string(),
            id: None,
            catalog_type: Some(CatalogType::Iceberg),
            kind: QueryLineageRelationKind::Table,
        };
        let lineage = lineage(
            QueryLineageKind::Dml,
            source,
            relation("target", Some(20)),
            vec![edge(1, "a", 3, "x")],
        );

        let entries = build_log_entries(lineage, "query-1".into(), "INSERT".into(), 1);
        assert_eq!(entries[0].source.catalog_type, Some("ICEBERG"));
        assert_eq!(entries[0].source.address_kind, "NAME");
        assert_eq!(
            entries[0].source_to_target_columns,
            BTreeMap::from([("a".to_string(), vec!["3".to_string()])])
        );
    }

    fn lineage(
        kind: QueryLineageKind,
        source: QueryLineageRelation,
        target: QueryLineageRelation,
        columns: Vec<QueryLineageColumnEdge>,
    ) -> QueryLineage {
        QueryLineage {
            kind,
            targets: vec![LineageTarget {
                relation: target,
                sources: vec![LineageSource {
                    relation: source,
                    columns,
                }],
            }],
        }
    }

    fn relation(name: &str, id: Option<u64>) -> QueryLineageRelation {
        relation_with_kind(name, id, QueryLineageRelationKind::Table)
    }

    fn view_relation(name: &str, id: Option<u64>) -> QueryLineageRelation {
        relation_with_kind(name, id, QueryLineageRelationKind::View)
    }

    fn relation_with_kind(
        name: &str,
        id: Option<u64>,
        kind: QueryLineageRelationKind,
    ) -> QueryLineageRelation {
        let catalog_type =
            (kind != QueryLineageRelationKind::Stage).then_some(CatalogType::Default);
        QueryLineageRelation {
            catalog: "default".to_string(),
            database: "default".to_string(),
            name: name.to_string(),
            id,
            catalog_type,
            kind,
        }
    }

    fn edge(
        source_id: u32,
        source_name: &str,
        target_id: u32,
        target_name: &str,
    ) -> QueryLineageColumnEdge {
        QueryLineageColumnEdge {
            source: QueryLineageColumn {
                id: source_id,
                name: source_name.to_string(),
            },
            target: QueryLineageColumn {
                id: target_id,
                name: target_name.to_string(),
            },
        }
    }
}
