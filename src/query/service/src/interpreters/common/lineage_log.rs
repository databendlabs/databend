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

use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
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
use crate::sessions::TableContextProgress;
use crate::sessions::convert_query_log_timestamp;

const LINEAGE_LOG_TARGET: &str = "databend::log::lineage";

#[derive(Serialize)]
struct LineageLogEntry {
    operation: &'static str,
    event_time: i64,
    user_name: Option<String>,
    query_parameterized_hash: Option<String>,
    query_info: LineageQueryInfo,
    lineage_kind: &'static str,
    source: LineageEndpoint,
    target: LineageEndpoint,
    column_lineage_hash: String,
    column_lineage: Option<ColumnLineage>,
}

#[derive(Serialize)]
struct LineageObjectDeleteLogEntry {
    operation: &'static str,
    object_id: u64,
    query_id: String,
    event_time: i64,
}

#[derive(Serialize)]
struct LineageEdgeDeleteLogEntry {
    operation: &'static str,
    event_time: i64,
    query_id: String,
    source_lineage_key: String,
    target_lineage_key: String,
    lineage_kind: String,
    column_lineage_hash: String,
}

#[derive(Clone, Debug, Serialize)]
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct LineageEdgeIdentity {
    pub source_lineage_key: String,
    pub target_lineage_key: String,
    pub lineage_kind: String,
    pub column_lineage_hash: String,
}

#[derive(Clone, Debug)]
pub(crate) struct SemanticLineageEdge {
    lineage_kind: &'static str,
    source: LineageEndpoint,
    target: LineageEndpoint,
    column_lineage_hash: String,
    column_lineage: Option<ColumnLineage>,
}

impl SemanticLineageEdge {
    pub(crate) fn identity(&self) -> LineageEdgeIdentity {
        LineageEdgeIdentity {
            source_lineage_key: self.source.lineage_key.clone(),
            target_lineage_key: self.target.lineage_key.clone(),
            lineage_kind: self.lineage_kind.to_string(),
            column_lineage_hash: self.column_lineage_hash.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LineageProcessMetadata {
    pub event_time: i64,
    pub user_name: Option<String>,
    pub query_parameterized_hash: Option<String>,
    pub query_info: LineageQueryInfo,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct LineageQueryInfo {
    pub query_id: Option<String>,
    pub query_text: Option<String>,
    pub query_duration_ms: Option<i64>,
    pub written_rows: Option<u64>,
    pub scan_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfilled_at: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ColumnLineage {
    source_column_address_kind: &'static str,
    target_column_address_kind: &'static str,
    mappings: Vec<ColumnLineageMapping>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ColumnLineageMapping {
    target: ColumnIdentity,
    sources: Vec<ColumnIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ColumnIdentity {
    name: String,
    id: Option<u32>,
}

pub fn log_query_lineage(ctx: &Arc<QueryContext>) {
    if let Some(lineage) = ctx.get_query_lineage() {
        let process = LineageProcessMetadata {
            // Successful lineage logging runs after log_query_finished, so this is the query
            // finish event time. Keep a fallback for non-standard test or embedding paths.
            event_time: convert_query_log_timestamp(
                ctx.get_query_finish_time().unwrap_or_else(SystemTime::now),
            ),
            user_name: Some(
                ctx.get_current_user()
                    .map(|user| user.name)
                    .unwrap_or_default(),
            ),
            query_parameterized_hash: Some(ctx.get_query_parameterized_hash()),
            query_info: LineageQueryInfo {
                query_id: Some(ctx.get_id()),
                query_text: Some(ctx.get_query_str()),
                query_duration_ms: Some(ctx.get_query_duration_ms()),
                written_rows: Some(ctx.get_write_progress_value().rows as u64),
                scan_rows: Some(ctx.get_scan_progress_value().rows as u64),
                backfilled_at: None,
            },
        };

        for edge in build_semantic_edges(lineage) {
            match serialize_upsert_edge(edge, process.clone()) {
                Ok(json) => info!(target: LINEAGE_LOG_TARGET, "{}", json),
                Err(err) => warn!("Failed to serialize query lineage log: {:?}", err),
            }
        }
    }

    for json in ctx.take_pending_lineage_logs() {
        info!(target: LINEAGE_LOG_TARGET, "{}", json);
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
    };
    match serde_json::to_string(&entry) {
        Ok(json) => info!(target: LINEAGE_LOG_TARGET, "{}", json),
        Err(err) => warn!("Failed to serialize lineage deletion log: {:?}", err),
    }
}

fn lineage_enabled() -> bool {
    GlobalConfig::instance().lineage.enabled()
}

#[cfg(test)]
fn build_log_entries(
    lineage: QueryLineage,
    process: LineageProcessMetadata,
) -> Vec<LineageLogEntry> {
    build_semantic_edges(lineage)
        .into_iter()
        .map(|edge| into_log_entry(edge, process.clone()))
        .collect()
}

pub(crate) fn build_semantic_edges(lineage: QueryLineage) -> Vec<SemanticLineageEdge> {
    let lineage_kind = lineage_kind_name(lineage.kind);
    let mut entries = Vec::new();

    for lineage_target in lineage.targets {
        let target = endpoint_from_relation(lineage.kind, false, &lineage_target.relation);
        for lineage_source in lineage_target.sources {
            if is_ignored_system_source(&lineage_source.relation) {
                continue;
            }
            let source = endpoint_from_relation(lineage.kind, true, &lineage_source.relation);
            if source.lineage_key == target.lineage_key {
                continue;
            }

            // Stages are object-level lineage endpoints. Their file fields are not stable schema
            // columns, so COPY edges intentionally carry no column lineage.
            let (column_lineage, column_pairs) =
                if source.object_type == "STAGE" || target.object_type == "STAGE" {
                    (None, BTreeSet::new())
                } else {
                    let column_lineage =
                        build_column_lineage(&source, &target, lineage_source.columns.into_iter());
                    let column_pairs = canonical_column_pairs(&column_lineage);
                    (Some(column_lineage), column_pairs)
                };

            entries.push(SemanticLineageEdge {
                lineage_kind,
                source: source.clone(),
                target: target.clone(),
                column_lineage_hash: column_lineage_hash(column_lineage.as_ref(), &column_pairs),
                column_lineage,
            });
        }
    }

    entries
}

fn is_ignored_system_source(relation: &QueryLineageRelation) -> bool {
    relation.catalog_type == Some(CatalogType::Default)
        && relation.catalog.eq_ignore_ascii_case("default")
        && ["system", "information_schema"]
            .iter()
            .any(|database| relation.database.eq_ignore_ascii_case(database))
}

fn into_log_entry(edge: SemanticLineageEdge, process: LineageProcessMetadata) -> LineageLogEntry {
    LineageLogEntry {
        operation: "UPSERT_EDGE",
        event_time: process.event_time,
        user_name: process.user_name,
        query_parameterized_hash: process.query_parameterized_hash,
        query_info: process.query_info,
        lineage_kind: edge.lineage_kind,
        source: edge.source,
        target: edge.target,
        column_lineage_hash: edge.column_lineage_hash,
        column_lineage: edge.column_lineage,
    }
}

pub(crate) fn serialize_upsert_edge(
    edge: SemanticLineageEdge,
    process: LineageProcessMetadata,
) -> Result<String> {
    serde_json::to_string(&into_log_entry(edge, process))
        .map_err(|err| ErrorCode::Internal(format!("failed to serialize lineage edge: {err}")))
}

pub(crate) fn serialize_delete_edge(
    identity: LineageEdgeIdentity,
    event_time: i64,
    query_id: String,
) -> Result<String> {
    serde_json::to_string(&LineageEdgeDeleteLogEntry {
        operation: "DELETE_EDGE",
        event_time,
        query_id,
        source_lineage_key: identity.source_lineage_key,
        target_lineage_key: identity.target_lineage_key,
        lineage_kind: identity.lineage_kind,
        column_lineage_hash: identity.column_lineage_hash,
    })
    .map_err(|err| ErrorCode::Internal(format!("failed to serialize lineage deletion: {err}")))
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

fn column_address_kind(endpoint: &LineageEndpoint) -> &'static str {
    // View output column ids are query-plan ordinals rather than stable schema ids. Keep view
    // columns name-addressed even though the view object itself has a stable table id.
    if endpoint.object_type == "VIEW" || endpoint.address_kind == "NAME" {
        "NAME"
    } else {
        "ID"
    }
}

fn build_column_lineage(
    source: &LineageEndpoint,
    target: &LineageEndpoint,
    columns: impl Iterator<Item = databend_common_sql::QueryLineageColumnEdge>,
) -> ColumnLineage {
    let source_kind = column_address_kind(source);
    let target_kind = column_address_kind(target);
    let mut mappings =
        BTreeMap::<String, (ColumnIdentity, BTreeMap<String, ColumnIdentity>)>::new();

    for edge in columns {
        let source_column = column_identity(source_kind, edge.source);
        let target_column = column_identity(target_kind, edge.target);
        let source_key = canonical_column_address(source_kind, &source_column);
        let target_key = canonical_column_address(target_kind, &target_column);
        mappings
            .entry(target_key)
            .or_insert_with(|| (target_column, BTreeMap::new()))
            .1
            .insert(source_key, source_column);
    }

    ColumnLineage {
        source_column_address_kind: source_kind,
        target_column_address_kind: target_kind,
        mappings: mappings
            .into_values()
            .map(|(target, sources)| ColumnLineageMapping {
                target,
                sources: sources.into_values().collect(),
            })
            .collect(),
    }
}

fn column_identity(kind: &str, column: QueryLineageColumn) -> ColumnIdentity {
    ColumnIdentity {
        name: column.name,
        id: (kind == "ID").then_some(column.id),
    }
}

fn canonical_column_address(kind: &str, column: &ColumnIdentity) -> String {
    if kind == "ID" {
        column
            .id
            .expect("ID-addressed lineage column must have an id")
            .to_string()
    } else {
        column.name.clone()
    }
}

fn canonical_column_pairs(column_lineage: &ColumnLineage) -> BTreeSet<(String, String)> {
    let mut pairs = BTreeSet::new();
    for mapping in &column_lineage.mappings {
        let target =
            canonical_column_address(column_lineage.target_column_address_kind, &mapping.target);
        for source in &mapping.sources {
            pairs.insert((
                canonical_column_address(column_lineage.source_column_address_kind, source),
                target.clone(),
            ));
        }
    }
    pairs
}

fn column_lineage_hash(
    column_lineage: Option<&ColumnLineage>,
    column_pairs: &BTreeSet<(String, String)>,
) -> String {
    let mut hasher = Sha256::new();
    if let Some(column_lineage) = column_lineage {
        for value in [
            column_lineage.source_column_address_kind,
            column_lineage.target_column_address_kind,
        ] {
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
    }
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
    fn test_build_log_entries_uses_canonical_column_lineage() {
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

        let entries = build_log_entries(captured, process("query-1", 1));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, "UPSERT_EDGE");
        assert_eq!(
            entries[0].query_info.query_text.as_deref(),
            Some("INSERT INTO dst SELECT 'quoted'\nFROM src")
        );
        assert_eq!(
            entries[0].query_parameterized_hash.as_deref(),
            Some("parameterized-hash")
        );
        assert_eq!(entries[0].query_info.query_duration_ms, Some(123));
        assert_eq!(entries[0].query_info.written_rows, Some(10));
        assert_eq!(entries[0].query_info.scan_rows, Some(20));
        let json = serde_json::to_value(&entries[0]).expect("lineage entry must serialize");
        assert_eq!(json["user_name"], "test-user");
        assert_eq!(json["query_info"]["query_id"], "query-1");
        assert_eq!(
            json["query_info"]["query_text"],
            "INSERT INTO dst SELECT 'quoted'\nFROM src"
        );
        assert!(json.get("query_id").is_none());
        assert!(json.get("query_kind").is_none());
        assert_eq!(
            entries[0].column_lineage,
            Some(ColumnLineage {
                source_column_address_kind: "ID",
                target_column_address_kind: "ID",
                mappings: vec![ColumnLineageMapping {
                    target: ColumnIdentity {
                        name: "x".to_string(),
                        id: Some(3),
                    },
                    sources: vec![
                        ColumnIdentity {
                            name: "a".to_string(),
                            id: Some(1),
                        },
                        ColumnIdentity {
                            name: "b".to_string(),
                            id: Some(2),
                        },
                    ],
                }],
            })
        );

        let reordered = lineage(
            QueryLineageKind::Dml,
            relation("source", Some(10)),
            relation("target", Some(20)),
            vec![edge(2, "b", 3, "x"), edge(1, "a", 3, "x")],
        );
        let mut latest_process = process("query-2", 2);
        latest_process.query_info.query_text =
            Some("INSERT INTO dst SELECT b, a FROM src".to_string());
        latest_process.query_parameterized_hash = Some("different-parameterized-hash".to_string());
        latest_process.query_info.query_duration_ms = Some(456);
        latest_process.query_info.written_rows = Some(30);
        latest_process.query_info.scan_rows = Some(40);
        let reordered = build_log_entries(reordered, latest_process);
        assert_eq!(
            entries[0].column_lineage_hash,
            reordered[0].column_lineage_hash
        );

        let renamed = lineage(
            QueryLineageKind::Dml,
            relation("source", Some(10)),
            relation("target", Some(20)),
            vec![
                edge(2, "renamed_b", 3, "renamed_x"),
                edge(1, "renamed_a", 3, "renamed_x"),
            ],
        );
        let renamed = build_log_entries(renamed, process("query-3", 3));
        assert_eq!(
            entries[0].column_lineage_hash, renamed[0].column_lineage_hash,
            "display names must not affect an ID-addressed pattern"
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

        let entries = build_log_entries(lineage, process("query-1", 1));
        assert_eq!(entries[0].source.address_kind, "NAME");
        assert_eq!(entries[0].target.address_kind, "ID");
        assert_eq!(
            entries[0].column_lineage,
            Some(ColumnLineage {
                source_column_address_kind: "NAME",
                target_column_address_kind: "NAME",
                mappings: vec![ColumnLineageMapping {
                    target: ColumnIdentity {
                        name: "x".to_string(),
                        id: None,
                    },
                    sources: vec![ColumnIdentity {
                        name: "a".to_string(),
                        id: None,
                    }],
                }],
            })
        );
    }

    #[test]
    fn test_build_log_entries_skips_self_loop() {
        let same = relation("table", Some(10));
        let lineage = lineage(QueryLineageKind::Dml, same.clone(), same, vec![edge(
            1, "a", 1, "a",
        )]);
        assert!(build_log_entries(lineage, process("query-1", 1)).is_empty());
    }

    #[test]
    fn test_copy_stage_uses_name_addressing_without_column_lineage() {
        let lineage = lineage(
            QueryLineageKind::Dml,
            relation_with_kind("named_stage", None, QueryLineageRelationKind::Stage),
            relation("target", Some(20)),
            vec![edge(1, "file_col", 3, "x")],
        );

        let entries = build_log_entries(lineage, process("query-1", 1));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].source.object_type, "STAGE");
        assert_eq!(entries[0].source.address_kind, "NAME");
        assert_eq!(entries[0].source.catalog_type, None);
        assert_eq!(entries[0].source.lineage_key, "STAGE::NAME::named_stage");
        assert_eq!(entries[0].column_lineage, None);
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

        let entries = build_log_entries(lineage, process("query-1", 1));
        assert_eq!(entries[0].source.catalog_type, Some("ICEBERG"));
        assert_eq!(entries[0].source.address_kind, "NAME");
        assert_eq!(
            entries[0].column_lineage,
            Some(ColumnLineage {
                source_column_address_kind: "NAME",
                target_column_address_kind: "ID",
                mappings: vec![ColumnLineageMapping {
                    target: ColumnIdentity {
                        name: "x".to_string(),
                        id: Some(3),
                    },
                    sources: vec![ColumnIdentity {
                        name: "a".to_string(),
                        id: None,
                    }],
                }],
            })
        );
    }

    #[test]
    fn test_delete_edge_serializes_complete_identity() {
        let json = serialize_delete_edge(
            LineageEdgeIdentity {
                source_lineage_key: "TABLE::NAME::default.db.src".to_string(),
                target_lineage_key: "VIEW::ID::42".to_string(),
                lineage_kind: "CREATE_VIEW".to_string(),
                column_lineage_hash: "column-hash".to_string(),
            },
            123,
            "refresh-query".to_string(),
        )
        .unwrap();
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json["operation"], "DELETE_EDGE");
        assert_eq!(json["event_time"], 123);
        assert_eq!(json["query_id"], "refresh-query");
        assert_eq!(json["source_lineage_key"], "TABLE::NAME::default.db.src");
        assert_eq!(json["target_lineage_key"], "VIEW::ID::42");
        assert_eq!(json["lineage_kind"], "CREATE_VIEW");
        assert_eq!(json["column_lineage_hash"], "column-hash");
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

    fn process(query_id: &str, event_time: i64) -> LineageProcessMetadata {
        LineageProcessMetadata {
            event_time,
            user_name: Some("test-user".to_string()),
            query_parameterized_hash: Some("parameterized-hash".to_string()),
            query_info: LineageQueryInfo {
                query_id: Some(query_id.to_string()),
                query_text: Some("INSERT INTO dst SELECT 'quoted'\nFROM src".to_string()),
                query_duration_ms: Some(123),
                written_rows: Some(10),
                scan_rows: Some(20),
                backfilled_at: None,
            },
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
