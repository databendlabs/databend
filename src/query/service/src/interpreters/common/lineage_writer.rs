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

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table::is_temp_table_by_table_info;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::LineageApi;
use databend_common_meta_api::MergeLineageReq;
use databend_common_meta_app::schema::ColumnRef;
use databend_common_meta_app::schema::LineageColumn;
use databend_common_meta_app::schema::LineageDetail;
use databend_common_meta_app::schema::LineageIdentity;
use databend_common_meta_app::schema::LineageKind;
use databend_common_meta_app::schema::LineageObjectRef;
use databend_common_meta_app::schema::LineageObjectType;
use databend_common_meta_app::schema::LineageUpdate;
use databend_common_meta_app::schema::LineageUpdateMode;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::basic_callback;
use databend_common_sql::LineageUpstream;
use databend_common_sql::QueryLineage;
use databend_common_sql::QueryLineageColumnEdge;
use databend_common_sql::QueryLineageKind;
use databend_common_sql::QueryLineageRelation;
use databend_common_users::UserApiProvider;
use log::warn;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub async fn build_query_lineage_updates(
    ctx: &Arc<QueryContext>,
    target_table_info: Option<&TableInfo>,
) -> Result<Vec<LineageUpdate>> {
    let query_lineage = ctx.get_query_lineage();
    build_query_lineage_updates_with_lineage(
        ctx.as_ref(),
        query_lineage.as_deref(),
        target_table_info,
    )
    .await
}

pub async fn build_query_lineage_updates_with_lineage(
    ctx: &dyn TableContext,
    query_lineage: Option<&QueryLineage>,
    target_table_info: Option<&TableInfo>,
) -> Result<Vec<LineageUpdate>> {
    let Some(query_lineage) = query_lineage else {
        return Ok(vec![]);
    };

    build_lineage_updates(ctx, query_lineage, target_table_info).await
}

pub fn attach_query_lineage_on_finished(pipeline: &mut Pipeline, updates: Vec<LineageUpdate>) {
    let updates = updates
        .into_iter()
        .filter(|update| update.mode == LineageUpdateMode::Merge)
        .collect::<Vec<_>>();
    if updates.is_empty() {
        return;
    }

    pipeline.set_on_finished(basic_callback(move |info: &ExecutionInfo| {
        if info.res.is_ok() {
            let _: Result<()> = GlobalIORuntime::instance().block_on(async move {
                let meta = UserApiProvider::instance().get_meta_store_client();
                handle_lineage_persistence_result(
                    meta.merge_lineage(MergeLineageReq { updates }).await,
                )
            });
        }

        Ok(())
    }));
}

fn handle_lineage_persistence_result<E: std::fmt::Display>(
    result: std::result::Result<(), E>,
) -> Result<()> {
    if let Err(error) = result {
        warn!("failed to persist query lineage: {error}");
    }
    Ok(())
}

async fn build_lineage_updates(
    ctx: &dyn TableContext,
    query_lineage: &QueryLineage,
    target_table_info: Option<&TableInfo>,
) -> Result<Vec<LineageUpdate>> {
    let kind = lineage_kind(query_lineage.kind);
    let mode = lineage_update_mode(query_lineage.kind);
    let mut updates = Vec::new();

    for downstream in &query_lineage.downstreams {
        let downstream_relation = if let Some(table_info) = target_table_info {
            if !is_lineage_supported_table_info(table_info) {
                warn!(
                    "skip query lineage target relation: kind={:?}, relation={}, reason=unsupported table engine '{}'",
                    query_lineage.kind,
                    full_table_name(&downstream.relation),
                    table_info.engine(),
                );
                continue;
            }
            ResolvedRelation::from_table_info(&downstream.relation, table_info)
        } else if query_lineage.kind == QueryLineageKind::CreateView {
            ResolvedRelation::unresolved(downstream.relation.clone())
        } else {
            match ResolvedRelation::resolve(ctx, &downstream.relation, true).await? {
                ResolveRelationResult::Resolved(relation) => relation,
                ResolveRelationResult::Skipped(reason) => {
                    warn!(
                        "skip query lineage target relation: kind={:?}, relation={}, reason={}",
                        query_lineage.kind,
                        full_table_name(&downstream.relation),
                        reason,
                    );
                    continue;
                }
            }
        };

        for upstream in &downstream.upstreams {
            let require_upstream_id_match = query_lineage.kind != QueryLineageKind::CreateView;
            let upstream_relation =
                ResolvedRelation::resolve(ctx, &upstream.relation, require_upstream_id_match)
                    .await?;
            let upstream_relation = match upstream_relation {
                ResolveRelationResult::Resolved(relation) => relation,
                ResolveRelationResult::Skipped(reason) => {
                    warn!(
                        "skip query lineage upstream relation: kind={:?}, relation={}, reason={}",
                        query_lineage.kind,
                        full_table_name(&upstream.relation),
                        reason,
                    );
                    continue;
                }
            };
            if upstream_relation.is_same_object(&downstream_relation) {
                continue;
            }

            let detail = LineageDetail {
                kind: kind.clone(),
                last_query_id: Some(ctx.get_id()),
                updated_on: Utc::now(),
                column_lineage: column_lineage(
                    query_lineage.kind,
                    &upstream_relation,
                    &downstream_relation,
                    upstream,
                ),
            };

            updates.push(LineageUpdate {
                tenant_name: ctx.get_tenant().tenant_name().to_string(),
                upstream: upstream_relation.object_ref(query_lineage.kind, true),
                downstream: downstream_relation.object_ref(query_lineage.kind, false),
                detail,
                mode: mode.clone(),
            });
        }
    }

    Ok(updates)
}

#[derive(Clone)]
struct ResolvedRelation {
    relation: QueryLineageRelation,
    current_table_id: Option<u64>,
    stable_table_id: bool,
    column_ids_by_name: HashMap<String, u64>,
    column_names_by_id: HashMap<u64, String>,
}

enum ResolveRelationResult {
    Resolved(ResolvedRelation),
    Skipped(String),
}

impl ResolvedRelation {
    fn unresolved(relation: QueryLineageRelation) -> Self {
        Self {
            relation,
            current_table_id: None,
            stable_table_id: false,
            column_ids_by_name: HashMap::new(),
            column_names_by_id: HashMap::new(),
        }
    }

    fn from_table_info(relation: &QueryLineageRelation, table_info: &TableInfo) -> Self {
        let mut column_ids_by_name = HashMap::new();
        let mut column_names_by_id = HashMap::new();
        for field in table_info.schema().fields() {
            column_ids_by_name.insert(field.name().clone(), u64::from(field.column_id));
            column_names_by_id.insert(u64::from(field.column_id), field.name().clone());
        }

        Self {
            relation: relation.clone(),
            current_table_id: Some(table_info.ident.table_id),
            stable_table_id: has_stable_lineage_table_id(table_info),
            column_ids_by_name,
            column_names_by_id,
        }
    }

    async fn resolve(
        ctx: &dyn TableContext,
        relation: &QueryLineageRelation,
        require_id_match: bool,
    ) -> Result<ResolveRelationResult> {
        let table = match ctx
            .get_table(&relation.catalog, &relation.database, &relation.name)
            .await
        {
            Ok(table) => table,
            Err(error) if is_unknown_relation_error(&error) => {
                return Ok(ResolveRelationResult::Skipped(error.message()));
            }
            Err(error) => return Err(error),
        };
        if table.is_temp() {
            return Ok(ResolveRelationResult::Skipped(
                "temporary table is not captured".to_string(),
            ));
        }
        let table_info = table.get_table_info();
        if !is_lineage_supported_table_info(table_info) {
            return Ok(ResolveRelationResult::Skipped(format!(
                "unsupported table engine '{}'",
                table_info.engine()
            )));
        }
        let current_table_id = table_info.ident.table_id;
        let stable_table_id = has_stable_lineage_table_id(table_info);

        if require_id_match
            && stable_table_id
            && relation.id.is_some_and(|id| id != current_table_id)
        {
            return Ok(ResolveRelationResult::Skipped(format!(
                "table id changed, expected {}, got {}",
                relation.id.unwrap(),
                current_table_id
            )));
        }

        let mut column_ids_by_name = HashMap::new();
        let mut column_names_by_id = HashMap::new();
        for field in table.schema().fields() {
            column_ids_by_name.insert(field.name().clone(), u64::from(field.column_id));
            column_names_by_id.insert(u64::from(field.column_id), field.name().clone());
        }

        Ok(ResolveRelationResult::Resolved(Self {
            relation: relation.clone(),
            current_table_id: Some(current_table_id),
            stable_table_id,
            column_ids_by_name,
            column_names_by_id,
        }))
    }

    fn object_ref(&self, kind: QueryLineageKind, is_upstream: bool) -> LineageObjectRef {
        let identity = if self.uses_column_name_ref(kind, is_upstream) {
            LineageIdentity::Name {
                name: full_table_name(&self.relation),
            }
        } else {
            LineageIdentity::Id {
                id: self.current_table_id.unwrap().to_string(),
            }
        };

        LineageObjectRef {
            object_type: LineageObjectType::Table,
            identity,
        }
    }

    fn uses_column_name_ref(&self, kind: QueryLineageKind, is_upstream: bool) -> bool {
        (is_upstream && (kind == QueryLineageKind::CreateView || self.relation.id.is_none()))
            || !self.stable_table_id
            || self.current_table_id.is_none()
    }

    fn is_same_object(&self, other: &Self) -> bool {
        if self.stable_table_id && other.stable_table_id {
            if let (Some(self_id), Some(other_id)) = (self.current_table_id, other.current_table_id)
            {
                return self_id == other_id;
            }
        }

        self.relation.catalog == other.relation.catalog
            && self.relation.database == other.relation.database
            && self.relation.name == other.relation.name
    }
}

fn is_unknown_relation_error(error: &ErrorCode) -> bool {
    matches!(
        error.code(),
        ErrorCode::UNKNOWN_CATALOG
            | ErrorCode::UNKNOWN_DATABASE
            | ErrorCode::UNKNOWN_TABLE
            | ErrorCode::UNKNOWN_VIEW
    )
}

fn is_lineage_supported_table_info(table_info: &TableInfo) -> bool {
    !is_temp_table_by_table_info(table_info)
        && !matches!(
            table_info.engine().to_ascii_uppercase().as_str(),
            "MEMORY" | "DELTA"
        )
}

fn has_stable_lineage_table_id(table_info: &TableInfo) -> bool {
    table_info.ident.table_id != 0
        && !matches!(
            table_info.engine().to_ascii_uppercase().as_str(),
            "ICEBERG" | "HIVE"
        )
}

fn lineage_kind(kind: QueryLineageKind) -> LineageKind {
    match kind {
        QueryLineageKind::CreateView => LineageKind::View,
        QueryLineageKind::Ctas => LineageKind::Ctas,
        QueryLineageKind::Dml => LineageKind::DataMovement,
    }
}

fn lineage_update_mode(kind: QueryLineageKind) -> LineageUpdateMode {
    match kind {
        QueryLineageKind::Dml => LineageUpdateMode::Merge,
        QueryLineageKind::CreateView | QueryLineageKind::Ctas => LineageUpdateMode::Replace,
    }
}

fn column_lineage(
    kind: QueryLineageKind,
    upstream_relation: &ResolvedRelation,
    downstream_relation: &ResolvedRelation,
    upstream: &LineageUpstream,
) -> Vec<LineageColumn> {
    upstream
        .columns
        .iter()
        .filter_map(
            |edge| match column_edge(kind, upstream_relation, downstream_relation, edge) {
                Ok(column) => Some(column),
                Err(reason) => {
                    warn!(
                        "skip query lineage column edge: kind={:?}, upstream_relation={}, downstream_relation={}, upstream_column={}({}), downstream_column={}({}), reason={}",
                        kind,
                        full_table_name(&upstream_relation.relation),
                        full_table_name(&downstream_relation.relation),
                        edge.upstream.name,
                        edge.upstream.id,
                        edge.downstream.name,
                        edge.downstream.id,
                        reason,
                    );
                    None
                }
            },
        )
        .collect()
}

fn column_edge(
    kind: QueryLineageKind,
    upstream_relation: &ResolvedRelation,
    downstream_relation: &ResolvedRelation,
    edge: &QueryLineageColumnEdge,
) -> std::result::Result<LineageColumn, String> {
    let upstream = if upstream_relation.uses_column_name_ref(kind, true) {
        upstream_relation
            .column_ids_by_name
            .contains_key(&edge.upstream.name)
            .then(|| ColumnRef::Name(edge.upstream.name.clone()))
            .ok_or_else(|| format!("upstream column '{}' is not found", edge.upstream.name))?
    } else {
        upstream_relation
            .column_names_by_id
            .contains_key(&u64::from(edge.upstream.id))
            .then_some(ColumnRef::Id(u64::from(edge.upstream.id)))
            .ok_or_else(|| format!("upstream column id {} is not found", edge.upstream.id))?
    };

    let downstream = if downstream_relation.uses_column_name_ref(kind, false) {
        (downstream_relation.column_ids_by_name.is_empty()
            || downstream_relation
                .column_ids_by_name
                .contains_key(&edge.downstream.name))
        .then(|| ColumnRef::Name(edge.downstream.name.clone()))
        .ok_or_else(|| format!("downstream column '{}' is not found", edge.downstream.name))?
    } else {
        downstream_relation
            .column_ids_by_name
            .get(&edge.downstream.name)
            .copied()
            .or_else(|| {
                downstream_relation
                    .column_names_by_id
                    .contains_key(&u64::from(edge.downstream.id))
                    .then_some(u64::from(edge.downstream.id))
            })
            .map(ColumnRef::Id)
            .ok_or_else(|| {
                format!(
                    "downstream column '{}'({}) is not found",
                    edge.downstream.name, edge.downstream.id
                )
            })?
    };

    Ok(LineageColumn {
        upstream,
        downstream,
    })
}

fn full_table_name(relation: &QueryLineageRelation) -> String {
    match relation.catalog.as_str() {
        "default" => format!("{}.{}", relation.database, relation.name),
        _ => format!(
            "{}.{}.{}",
            relation.catalog, relation.database, relation.name
        ),
    }
}

#[cfg(test)]
mod tests {
    use databend_common_sql::QueryLineageColumn;
    use databend_common_sql::QueryLineageRelationKind;

    use super::*;

    #[test]
    fn test_column_lineage_skips_unexpressible_column_edge() {
        let upstream = resolved_relation(Some(1), true, vec![("a", 11)]);
        let downstream = resolved_relation(Some(2), true, vec![("b", 22)]);
        let from = LineageUpstream {
            relation: upstream.relation.clone(),
            columns: vec![QueryLineageColumnEdge {
                upstream: column("a", 11),
                downstream: column("missing", 999),
            }],
        };

        let got = column_lineage(QueryLineageKind::Ctas, &upstream, &downstream, &from);

        assert!(got.is_empty());
    }

    #[test]
    fn test_column_lineage_keeps_object_edge_when_columns_are_empty() {
        let upstream = resolved_relation(Some(1), true, vec![("a", 11)]);
        let downstream = resolved_relation(Some(2), true, vec![("b", 22)]);
        let from = LineageUpstream {
            relation: upstream.relation.clone(),
            columns: vec![],
        };

        let got = column_lineage(QueryLineageKind::Ctas, &upstream, &downstream, &from);

        assert!(got.is_empty());
    }

    #[test]
    fn test_column_edge_uses_name_for_unresolved_downstream() {
        let upstream = resolved_relation(Some(1), true, vec![("a", 11)]);
        let downstream = resolved_relation(None, false, vec![]);
        let edge = QueryLineageColumnEdge {
            upstream: column("a", 11),
            downstream: column("v", 101),
        };

        let got = column_edge(QueryLineageKind::CreateView, &upstream, &downstream, &edge)
            .expect("unresolved view target should use column name");

        assert_eq!(got.upstream, ColumnRef::Name("a".to_string()));
        assert_eq!(got.downstream, ColumnRef::Name("v".to_string()));
    }

    #[test]
    fn test_lineage_supported_table_engines() {
        let mut memory = TableInfo::default();
        memory.meta.engine = "MEMORY".to_string();
        assert!(!is_lineage_supported_table_info(&memory));

        let mut delta = TableInfo::default();
        delta.meta.engine = "DELTA".to_string();
        assert!(!is_lineage_supported_table_info(&delta));

        let mut fuse = TableInfo::default();
        fuse.meta.engine = "FUSE".to_string();
        assert!(is_lineage_supported_table_info(&fuse));
    }

    #[test]
    fn test_unknown_relation_errors_are_skippable() {
        assert!(is_unknown_relation_error(&ErrorCode::UnknownTable("t")));
        assert!(is_unknown_relation_error(&ErrorCode::UnknownDatabase("db")));
        assert!(is_unknown_relation_error(&ErrorCode::UnknownCatalog(
            "catalog"
        )));
        assert!(!is_unknown_relation_error(&ErrorCode::MetaServiceError(
            "meta error"
        )));
    }

    #[test]
    fn test_lineage_persistence_failure_is_best_effort() {
        assert!(handle_lineage_persistence_result::<&str>(Err("meta unavailable")).is_ok());
    }

    #[test]
    fn test_same_lineage_object_prefers_stable_table_id() {
        let relation = resolved_relation(Some(1), true, vec![]);
        let same = resolved_relation(Some(1), true, vec![]);
        let replaced = resolved_relation(Some(2), true, vec![]);

        assert!(relation.is_same_object(&same));
        assert!(!relation.is_same_object(&replaced));
    }

    #[test]
    fn test_same_lineage_object_falls_back_to_name() {
        let relation = resolved_relation(None, false, vec![]);
        let same = resolved_relation(None, false, vec![]);
        let mut other = resolved_relation(None, false, vec![]);
        other.relation.name = "other".to_string();

        assert!(relation.is_same_object(&same));
        assert!(!relation.is_same_object(&other));
    }

    fn resolved_relation(
        current_table_id: Option<u64>,
        stable_table_id: bool,
        columns: Vec<(&str, u64)>,
    ) -> ResolvedRelation {
        let mut column_ids_by_name = HashMap::new();
        let mut column_names_by_id = HashMap::new();
        for (name, id) in columns {
            column_ids_by_name.insert(name.to_string(), id);
            column_names_by_id.insert(id, name.to_string());
        }

        ResolvedRelation {
            relation: relation("t", current_table_id),
            current_table_id,
            stable_table_id,
            column_ids_by_name,
            column_names_by_id,
        }
    }

    fn relation(name: &str, id: Option<u64>) -> QueryLineageRelation {
        QueryLineageRelation {
            catalog: "default".to_string(),
            database: "db".to_string(),
            name: name.to_string(),
            id,
            kind: QueryLineageRelationKind::Table,
        }
    }

    fn column(name: &str, id: u64) -> QueryLineageColumn {
        QueryLineageColumn {
            name: name.to_string(),
            id: id as u32,
        }
    }
}
