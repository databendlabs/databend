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

use chrono::SecondsFormat;
use chrono::Utc;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::database::is_system_database;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::CatalogType;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_sql::Planner;
use databend_common_sql::QueryLineage;
use databend_common_sql::QueryLineageRelation;
use databend_common_sql::QueryLineageRelationKind;
use databend_common_sql::plans::RefreshLineagePlan;
use databend_common_sql::plans::RefreshLineageSelector;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;

use crate::interpreters::Interpreter;
use crate::interpreters::common::LineageEdgeIdentity;
use crate::interpreters::common::LineageProcessMetadata;
use crate::interpreters::common::LineageQueryInfo;
use crate::interpreters::common::SemanticLineageEdge;
use crate::interpreters::common::build_semantic_edges;
use crate::interpreters::common::serialize_delete_edge;
use crate::interpreters::common::serialize_upsert_edge;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContextTableAccess;
use crate::table_functions::LineageEdgeReader;
use crate::table_functions::RawLineageEdge;

const DEFAULT_CATALOG: &str = "default";

pub struct RefreshLineageInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshLineagePlan,
}

struct ViewEntry {
    database: String,
    name: String,
    table_id: u64,
    created_on: i64,
    query: Option<String>,
}

struct RefreshResult {
    object_domain: &'static str,
    catalog: Option<String>,
    database: Option<String>,
    object_name: String,
    status: &'static str,
    edge_count: u64,
    upsert_count: u64,
    delete_count: u64,
    error: Option<String>,
}

struct ViewReconciliation {
    edge_count: u64,
    upserts: Vec<SemanticLineageEdge>,
    deletes: Vec<LineageEdgeIdentity>,
    process_seed: Option<ExistingProcess>,
}

struct ExistingLineageEdge {
    identity: LineageEdgeIdentity,
    process: ExistingProcess,
}

#[derive(Clone, Default)]
struct ExistingProcess {
    updated_on: Option<i64>,
    user_name: Option<String>,
    query_parameterized_hash: Option<String>,
    query_id: Option<String>,
    query_text: Option<String>,
    query_duration_ms: Option<i64>,
    written_rows: Option<u64>,
    scan_rows: Option<u64>,
}

impl ExistingProcess {
    fn newer_than(&self, other: &Self) -> bool {
        (
            self.updated_on,
            self.query_id.as_deref().unwrap_or_default(),
        ) > (
            other.updated_on,
            other.query_id.as_deref().unwrap_or_default(),
        )
    }
}

struct CurrentCatalogGuard {
    session: Arc<Session>,
    original: String,
}

impl CurrentCatalogGuard {
    fn set_default(ctx: &QueryContext) -> Self {
        let session = ctx.get_current_session();
        let original = session.get_current_catalog();
        session.set_current_catalog(DEFAULT_CATALOG.to_string());
        Self { session, original }
    }
}

impl Drop for CurrentCatalogGuard {
    fn drop(&mut self) {
        self.session.set_current_catalog(self.original.clone());
    }
}

impl RefreshLineageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshLineagePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }

    async fn list_views(&self, catalog: &dyn Catalog) -> Result<Vec<ViewEntry>> {
        let tenant = self.ctx.get_tenant();
        let mut views = Vec::new();
        for database in catalog.list_databases(&tenant).await? {
            let database_name = database.name().to_string();
            if is_system_database(&database_name) {
                continue;
            }
            for table in catalog.list_tables(&tenant, &database_name).await? {
                if !table.engine().eq_ignore_ascii_case(VIEW_ENGINE) {
                    continue;
                }
                let table_info = table.get_table_info();
                if table_info.db_type != DatabaseType::NormalDB {
                    continue;
                }
                views.push(ViewEntry {
                    database: database_name.clone(),
                    name: table.name().to_string(),
                    table_id: table_info.ident.table_id,
                    created_on: table_info.meta.created_on.timestamp_micros(),
                    query: table_info.meta.options.get(QUERY).cloned(),
                });
            }
        }
        views.sort_by(|left, right| {
            (&left.database, &left.name).cmp(&(&right.database, &right.name))
        });
        Ok(views)
    }

    async fn extract_view_lineage(
        &self,
        catalog: &dyn Catalog,
        view: &ViewEntry,
    ) -> Result<QueryLineage> {
        let query = view.query.as_deref().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "View '{}.{}' has no stored query",
                view.database, view.name
            ))
        })?;
        let mut planner = Planner::new(self.ctx.clone());
        let (query_plan, _) = planner.plan_sql(query).await?;
        let lineage = query_plan.query_lineage_for_view(QueryLineageRelation {
            catalog: DEFAULT_CATALOG.to_string(),
            database: view.database.clone(),
            name: view.name.clone(),
            id: Some(view.table_id),
            catalog_type: Some(CatalogType::Default),
            kind: QueryLineageRelationKind::View,
        })?;

        let current = catalog
            .get_table(&self.ctx.get_tenant(), &view.database, &view.name)
            .await?;
        if !current.engine().eq_ignore_ascii_case(VIEW_ENGINE)
            || current.get_table_info().ident.table_id != view.table_id
        {
            return Err(ErrorCode::Internal(format!(
                "View '{}.{}' was replaced while refreshing lineage",
                view.database, view.name
            )));
        }

        Ok(lineage)
    }

    fn existing_edge(edge: &RawLineageEdge) -> Option<ExistingLineageEdge> {
        let lineage_kind = edge.lineage_kind.clone()?;
        (lineage_kind == "CREATE_VIEW").then(|| ExistingLineageEdge {
            identity: LineageEdgeIdentity {
                source_lineage_key: edge.source.lineage_key.clone(),
                target_lineage_key: edge.target.lineage_key.clone(),
                lineage_kind,
                column_lineage_hash: edge.column_lineage_hash.clone(),
            },
            process: ExistingProcess {
                updated_on: edge.updated_on,
                user_name: edge.user_name.clone(),
                query_parameterized_hash: edge.query_parameterized_hash.clone(),
                query_id: edge.query_info.query_id.clone(),
                query_text: edge.query_info.query_text.clone(),
                query_duration_ms: edge.query_info.query_duration_ms,
                written_rows: edge.query_info.written_rows,
                scan_rows: edge.query_info.scan_rows,
            },
        })
    }

    fn reconcile(
        desired: Vec<SemanticLineageEdge>,
        existing: Vec<ExistingLineageEdge>,
    ) -> ViewReconciliation {
        let desired = desired
            .into_iter()
            .map(|edge| (edge.identity(), edge))
            .collect::<BTreeMap<_, _>>();
        let mut current = BTreeMap::<LineageEdgeIdentity, ExistingProcess>::new();
        for edge in existing {
            match current.get_mut(&edge.identity) {
                Some(current_process) if edge.process.newer_than(current_process) => {
                    *current_process = edge.process;
                }
                None => {
                    current.insert(edge.identity, edge.process);
                }
                _ => {}
            }
        }

        let process_seed = current
            .values()
            .max_by(|left, right| {
                if left.newer_than(right) {
                    std::cmp::Ordering::Greater
                } else if right.newer_than(left) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .cloned();
        let upserts = desired
            .iter()
            .filter(|(identity, _)| !current.contains_key(*identity))
            .map(|(_, edge)| edge.clone())
            .collect();
        let deletes = current
            .keys()
            .filter(|identity| !desired.contains_key(*identity))
            .cloned()
            .collect();

        ViewReconciliation {
            edge_count: desired.len() as u64,
            upserts,
            deletes,
            process_seed,
        }
    }

    fn canonical_create_view(view: &ViewEntry) -> String {
        let quote = |name: &str| format!("`{}`", name.replace('`', "``"));
        format!(
            "CREATE VIEW {}.{}.{} AS {}",
            quote(DEFAULT_CATALOG),
            quote(&view.database),
            quote(&view.name),
            view.query.as_deref().unwrap_or_default()
        )
    }

    fn backfill_process(
        view: &ViewEntry,
        seed: Option<&ExistingProcess>,
        backfilled_at: &str,
    ) -> LineageProcessMetadata {
        // A refresh is capture provenance, not the data-producing process. Preserve the original
        // CREATE VIEW snapshot when one exists and keep the refresh time in backfilled_at only.
        LineageProcessMetadata {
            event_time: seed
                .and_then(|process| process.updated_on)
                .unwrap_or(view.created_on),
            user_name: seed.and_then(|process| process.user_name.clone()),
            query_parameterized_hash: seed
                .and_then(|process| process.query_parameterized_hash.clone()),
            query_info: LineageQueryInfo {
                query_id: seed.and_then(|process| process.query_id.clone()),
                query_text: seed
                    .and_then(|process| process.query_text.clone())
                    .or_else(|| Some(Self::canonical_create_view(view))),
                query_duration_ms: seed.and_then(|process| process.query_duration_ms),
                written_rows: seed.and_then(|process| process.written_rows),
                scan_rows: seed.and_then(|process| process.scan_rows),
                backfilled_at: Some(backfilled_at.to_string()),
            },
        }
    }

    fn result_block(results: Vec<RefreshResult>) -> DataBlock {
        let mut object_domains = Vec::with_capacity(results.len());
        let mut catalogs = Vec::with_capacity(results.len());
        let mut databases = Vec::with_capacity(results.len());
        let mut object_names = Vec::with_capacity(results.len());
        let mut statuses = Vec::with_capacity(results.len());
        let mut edge_counts = Vec::with_capacity(results.len());
        let mut upsert_counts = Vec::with_capacity(results.len());
        let mut delete_counts = Vec::with_capacity(results.len());
        let mut errors = Vec::with_capacity(results.len());
        for result in results {
            object_domains.push(result.object_domain.to_string());
            catalogs.push(result.catalog);
            databases.push(result.database);
            object_names.push(result.object_name);
            statuses.push(result.status.to_string());
            edge_counts.push(result.edge_count);
            upsert_counts.push(result.upsert_count);
            delete_counts.push(result.delete_count);
            errors.push(result.error);
        }
        DataBlock::new_from_columns(vec![
            StringType::from_data(object_domains),
            StringType::from_opt_data(catalogs),
            StringType::from_opt_data(databases),
            StringType::from_data(object_names),
            StringType::from_data(statuses),
            UInt64Type::from_data(edge_counts),
            UInt64Type::from_data(upsert_counts),
            UInt64Type::from_data(delete_counts),
            StringType::from_opt_data(errors),
        ])
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshLineageInterpreter {
    fn name(&self) -> &str {
        "RefreshLineageInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if !GlobalConfig::instance().lineage.enabled() {
            return Err(ErrorCode::InvalidConfig(
                "REFRESH LINEAGE requires lineage to be enabled".to_string(),
            ));
        }

        let catalog = self.ctx.get_catalog(DEFAULT_CATALOG).await?;
        // Stored default-catalog View SQL commonly contains `database.table`. Make its original
        // catalog context explicit for planning and restore the caller's session on every exit.
        let _catalog_guard = CurrentCatalogGuard::set_default(self.ctx.as_ref());
        let views = match self.plan.selector {
            RefreshLineageSelector::AllViews => self.list_views(catalog.as_ref()).await?,
        };
        let target_keys = views
            .iter()
            .map(|view| format!("VIEW::ID::{}", view.table_id))
            .collect::<BTreeSet<_>>();
        let mut reader: LineageEdgeReader = LineageEdgeReader::try_create(self.ctx.clone()).await?;
        let existing_edges = reader
            .read_frontier("target_lineage_key", &target_keys)
            .await?;
        let mut existing_by_target = BTreeMap::<String, Vec<RawLineageEdge>>::new();
        for edge in existing_edges {
            existing_by_target
                .entry(edge.target.lineage_key.clone())
                .or_default()
                .push(edge);
        }

        let refresh_time = Utc::now();
        let refresh_event_time = refresh_time.timestamp_micros();
        let backfilled_at = refresh_time.to_rfc3339_opts(SecondsFormat::Micros, true);
        let query_id = self.ctx.get_id();
        let mut results = Vec::new();
        let mut pending_logs = Vec::new();
        for view in views {
            match self.extract_view_lineage(catalog.as_ref(), &view).await {
                Ok(lineage) => {
                    let target_key = format!("VIEW::ID::{}", view.table_id);
                    let existing = existing_by_target
                        .get(&target_key)
                        .into_iter()
                        .flatten()
                        .filter_map(Self::existing_edge)
                        .collect();
                    let reconciliation = Self::reconcile(build_semantic_edges(lineage), existing);
                    let upsert_count = reconciliation.upserts.len() as u64;
                    let delete_count = reconciliation.deletes.len() as u64;
                    // Large tenants commonly have mostly unchanged Views. Successful no-op
                    // objects are intentionally omitted from the result set.
                    if upsert_count == 0 && delete_count == 0 {
                        continue;
                    }

                    if !self.plan.dry_run {
                        let process = Self::backfill_process(
                            &view,
                            reconciliation.process_seed.as_ref(),
                            &backfilled_at,
                        );
                        for edge in reconciliation.upserts {
                            pending_logs.push(serialize_upsert_edge(edge, process.clone())?);
                        }
                        for identity in reconciliation.deletes {
                            pending_logs.push(serialize_delete_edge(
                                identity,
                                refresh_event_time,
                                query_id.clone(),
                            )?);
                        }
                    }

                    results.push(RefreshResult {
                        object_domain: "VIEW",
                        catalog: Some(DEFAULT_CATALOG.to_string()),
                        database: Some(view.database),
                        object_name: view.name,
                        status: if self.plan.dry_run {
                            "DRY_RUN"
                        } else {
                            "REFRESHED"
                        },
                        edge_count: reconciliation.edge_count,
                        upsert_count,
                        delete_count,
                        error: None,
                    });
                }
                Err(error) => results.push(RefreshResult {
                    object_domain: "VIEW",
                    catalog: Some(DEFAULT_CATALOG.to_string()),
                    database: Some(view.database),
                    object_name: view.name,
                    status: "ERROR",
                    edge_count: 0,
                    upsert_count: 0,
                    delete_count: 0,
                    error: Some(error.to_string()),
                }),
            }
        }

        self.ctx.attach_query_lineage(None);
        if !self.plan.dry_run {
            self.ctx.attach_pending_lineage_logs(pending_logs);
        }

        PipelineBuildResult::from_blocks(vec![Self::result_block(results)])
    }
}

#[cfg(test)]
mod tests {
    use databend_common_sql::LineageSource;
    use databend_common_sql::LineageTarget;
    use databend_common_sql::QueryLineageColumn;
    use databend_common_sql::QueryLineageColumnEdge;
    use databend_common_sql::QueryLineageKind;

    use super::*;

    #[test]
    fn test_reconcile_unchanged_missing_and_hash_change() {
        let desired = desired_edge("src", "a", "x");
        let identity = desired.identity();

        let unchanged =
            RefreshLineageInterpreter::reconcile(vec![desired.clone()], vec![existing_edge(
                identity.clone(),
                10,
            )]);
        assert_eq!(unchanged.edge_count, 1);
        assert!(unchanged.upserts.is_empty());
        assert!(unchanged.deletes.is_empty());

        let missing = RefreshLineageInterpreter::reconcile(vec![desired.clone()], vec![]);
        assert_eq!(missing.upserts.len(), 1);
        assert!(missing.deletes.is_empty());

        let mut old_identity = identity;
        old_identity.column_lineage_hash = "old-column-hash".to_string();
        let changed = RefreshLineageInterpreter::reconcile(vec![desired], vec![existing_edge(
            old_identity.clone(),
            10,
        )]);
        assert_eq!(changed.edge_count, 1);
        assert_eq!(changed.upserts.len(), 1);
        assert_eq!(changed.deletes, vec![old_identity]);
    }

    #[test]
    fn test_reconcile_removes_stale_edges_and_keeps_latest_process() {
        let desired_a = desired_edge("src_a", "a", "x");
        let desired_b = desired_edge("src_b", "b", "y");
        let stale = desired_edge("old_src", "c", "z").identity();
        let existing = vec![
            existing_edge(desired_a.identity(), 10),
            existing_edge(desired_b.identity(), 20),
            existing_edge(stale.clone(), 15),
        ];

        let reconciliation =
            RefreshLineageInterpreter::reconcile(vec![desired_a, desired_b], existing);
        assert_eq!(reconciliation.edge_count, 2);
        assert!(reconciliation.upserts.is_empty());
        assert_eq!(reconciliation.deletes, vec![stale]);
        assert_eq!(
            reconciliation
                .process_seed
                .as_ref()
                .and_then(|process| process.updated_on),
            Some(20)
        );

        let constant = RefreshLineageInterpreter::reconcile(vec![], vec![]);
        assert_eq!(constant.edge_count, 0);
        assert!(constant.upserts.is_empty());
        assert!(constant.deletes.is_empty());
    }

    fn desired_edge(
        source_name: &str,
        source_column: &str,
        target_column: &str,
    ) -> SemanticLineageEdge {
        build_semantic_edges(QueryLineage {
            kind: QueryLineageKind::CreateView,
            targets: vec![LineageTarget {
                relation: QueryLineageRelation {
                    catalog: DEFAULT_CATALOG.to_string(),
                    database: "db".to_string(),
                    name: "view".to_string(),
                    id: Some(42),
                    catalog_type: Some(CatalogType::Default),
                    kind: QueryLineageRelationKind::View,
                },
                sources: vec![LineageSource {
                    relation: QueryLineageRelation {
                        catalog: DEFAULT_CATALOG.to_string(),
                        database: "db".to_string(),
                        name: source_name.to_string(),
                        id: Some(10),
                        catalog_type: Some(CatalogType::Default),
                        kind: QueryLineageRelationKind::Table,
                    },
                    columns: vec![QueryLineageColumnEdge {
                        source: QueryLineageColumn {
                            id: 1,
                            name: source_column.to_string(),
                        },
                        target: QueryLineageColumn {
                            id: 2,
                            name: target_column.to_string(),
                        },
                    }],
                }],
            }],
        })
        .pop()
        .unwrap()
    }

    fn existing_edge(identity: LineageEdgeIdentity, updated_on: i64) -> ExistingLineageEdge {
        ExistingLineageEdge {
            identity,
            process: existing_process(updated_on),
        }
    }

    fn existing_process(updated_on: i64) -> ExistingProcess {
        ExistingProcess {
            updated_on: Some(updated_on),
            ..Default::default()
        }
    }
}
