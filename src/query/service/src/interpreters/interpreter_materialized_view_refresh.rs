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

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::InsertOperation;
use databend_common_ast::ast::InsertSource;
use databend_common_ast::ast::InsertStmt;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::MergeIntoStmt;
use databend_common_ast::ast::MergeOption;
use databend_common_ast::ast::MutationSource;
use databend_common_ast::ast::MutationUpdateExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableRef;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnmatchedClause;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContextSession;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_ENGINE;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_sql::MaterializedViewChecker;
use databend_common_sql::Planner;
use databend_common_sql::parse_materialized_view_query;
use databend_common_sql::plans::RefreshMaterializedViewPlan;
use databend_common_sql::validate_materialized_view_source;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::ChangesDesc;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_BASE_BLOCKS;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_DELTA_BLOCKS;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::StreamMode;
use log::info;
use log::warn;

use crate::interpreters::InsertInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::MutationInterpreter;
use crate::interpreters::common::QueryFinishHooks;
use crate::interpreters::interpreter_txn_commit::execute_commit_statement;
use crate::locks::LockManager;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

// Full-table semantic compaction is expensive, so wait until incremental aggregate refreshes
// have appended enough state blocks. Large MVs additionally require at least 10% block growth.
const AGGREGATE_MV_COMPACT_MIN_DELTA_BLOCKS: u64 = 32;

#[derive(Clone, Copy, Eq, PartialEq)]
enum AggregateRefreshEffect {
    None,
    Rebuilt,
    Appended,
}

pub struct RefreshMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshMaterializedViewPlan,
}

impl RefreshMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshMaterializedViewPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "RefreshMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let table = self
            .ctx
            .get_table(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.view_name,
            )
            .await?;
        if table.engine() != MATERIALIZED_VIEW_ENGINE {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} is not a materialized view",
                self.plan.database, self.plan.view_name
            )));
        }

        // MV refresh consumes a checkpoint range exactly once, and its optional semantic
        // compaction updates the same storage afterwards. Hold a mandatory, non-waiting table
        // lock across both phases so a concurrent REFRESH fails instead of consuming the same
        // source changes or racing with compaction. This deliberately bypasses enable_table_lock:
        // for MV refresh the lock is a correctness requirement, not an optional optimization.
        let locked_table_id = table.get_id();
        let table_lock = LockManager::create_table_lock(table.get_table_info().clone())?;
        let _lock_guard = table_lock.try_lock(self.ctx.clone(), false).await?;

        // The table may have changed while the lock was being acquired. Reload it after locking so
        // checkpoint and snapshot reads use the endpoint protected by this guard.
        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.view_name,
        )?;
        let table = self
            .ctx
            .get_table(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.view_name,
            )
            .await?;
        if table.get_id() != locked_table_id || table.engine() != MATERIALIZED_VIEW_ENGINE {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {}.{} changed while acquiring its refresh lock",
                self.plan.database, self.plan.view_name
            )));
        }

        let table = FuseTable::try_from_table(table.as_ref())?;
        MaterializedViewRefresh::new(table, self.ctx.clone(), &self.plan)
            .execute()
            .await?;
        Ok(PipelineBuildResult::create())
    }
}

struct MaterializedViewRefresh<'a> {
    mv_table: &'a FuseTable,
    ctx: Arc<QueryContext>,
    plan: &'a RefreshMaterializedViewPlan,
}

impl<'a> MaterializedViewRefresh<'a> {
    fn new(
        mv_table: &'a FuseTable,
        ctx: Arc<QueryContext>,
        plan: &'a RefreshMaterializedViewPlan,
    ) -> Self {
        Self {
            mv_table,
            ctx,
            plan,
        }
    }

    async fn execute(&self) -> Result<()> {
        let mv_meta = &self.mv_table.get_table_info().meta;
        let source_table_id = mv_meta
            .materialized_view_source_table_id()
            .map_err(ErrorCode::from)?;
        let checkpoint = match (
            mv_meta
                .options
                .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ),
            mv_meta
                .options
                .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION),
        ) {
            (None, None) => None,
            (Some(seq), location) => Some((
                seq.parse::<u64>().map_err(|error| {
                    ErrorCode::InvalidMaterializedView(format!(
                        "invalid materialized view source offset '{seq}': {error}"
                    ))
                })?,
                location.cloned(),
            )),
            (None, Some(_)) => {
                return Err(ErrorCode::InvalidMaterializedView(
                    "materialized view source checkpoint is incomplete",
                ));
            }
        };

        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let definition = catalog
            .get_mv_definition(&self.plan.tenant, self.mv_table.get_id())
            .await?
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView("materialized view definition not found")
            })?;
        self.validate_source_table_id(&definition.data.query, source_table_id)
            .await?;
        let source_meta = catalog
            .get_table_meta_by_id(source_table_id)
            .await?
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(format!(
                    "materialized view {}.{} source table changed: expected table id {} no longer exists",
                    self.plan.database, self.plan.view_name, source_table_id
                ))
            })?;
        let source_seq = source_meta.seq;
        let source_snapshot_location = source_meta
            .data
            .options
            .get(OPT_KEY_SNAPSHOT_LOCATION)
            .cloned();
        let change_tracking_enabled = source_meta
            .data
            .options
            .get(OPT_KEY_CHANGE_TRACKING)
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
        if !change_tracking_enabled {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {}.{} source table id {} does not have CHANGE_TRACKING enabled",
                self.plan.database, self.plan.view_name, source_table_id
            )));
        }
        if checkpoint
            .as_ref()
            .is_some_and(|(mv_source_seq, _)| *mv_source_seq > source_seq)
        {
            let mv_source_seq = checkpoint.as_ref().unwrap().0;
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {}.{} offset {} is newer than source table version {}",
                self.plan.database, self.plan.view_name, mv_source_seq, source_seq
            )));
        }

        if checkpoint.as_ref() == Some(&(source_seq, source_snapshot_location.clone())) {
            return Ok(());
        }

        info!(
            "materialized view {}.{} refresh offsets: source_table_id={}, mv_offset={:?}, source_offset={}, initial_refresh={}, changed={}",
            self.plan.database,
            self.plan.view_name,
            source_table_id,
            checkpoint.as_ref().map(|(seq, _)| seq),
            source_seq,
            checkpoint.is_none(),
            checkpoint
                .as_ref()
                .is_none_or(|(mv_source_seq, _)| *mv_source_seq != source_seq)
        );

        let logical_query = parse_materialized_view_query(
            &definition.data.original_query,
            "invalid materialized view logical query",
        )?;
        let is_aggregating = MaterializedViewChecker::check_query(&logical_query).is_aggregating();

        let source_database_id = source_meta
            .data
            .options
            .get(OPT_KEY_DATABASE_ID)
            .ok_or_else(|| ErrorCode::Internal("source table database id is missing"))?
            .parse::<u64>()?;
        let source_database = catalog.get_db_name_by_id(source_database_id).await?;
        let source_table_name = catalog
            .get_table_name_by_id(source_table_id)
            .await?
            .ok_or_else(|| {
                ErrorCode::UnknownTable(format!(
                    "materialized view source table id {} not found",
                    source_table_id
                ))
            })?;
        let current_source_table = catalog
            .get_table(&self.plan.tenant, &source_database, &source_table_name)
            .await?;
        let source_table = catalog.get_table_by_info(&TableInfo {
            ident: TableIdent::new(source_table_id, source_seq),
            meta: source_meta.data.clone(),
            ..current_source_table.get_table_info().clone()
        })?;
        let source_table = FuseTable::try_from_table(source_table.as_ref())?;
        if let Some((mv_source_seq, Some(_))) = &checkpoint {
            source_table
                .check_changes_valid(&source_table.get_table_info().desc, *mv_source_seq)?;
        }

        let previous_mv_block_count = self
            .mv_table
            .read_table_snapshot()
            .await?
            .map(|snapshot| snapshot.summary.block_count)
            .unwrap_or(0);
        let txn_mgr = self.ctx.txn_mgr();
        if txn_mgr.lock().is_active() {
            return Err(ErrorCode::InvalidOperation(
                "REFRESH MATERIALIZED VIEW cannot run inside an active transaction",
            ));
        }
        txn_mgr.lock().begin();
        let refresh_result = self
            .execute_in_transaction(
                source_table,
                &definition.data.query,
                &source_database,
                &source_table_name,
                is_aggregating,
                previous_mv_block_count,
                checkpoint,
                source_seq,
                source_snapshot_location,
            )
            .await;
        if refresh_result.is_err() {
            txn_mgr.lock().clear();
        }
        refresh_result
    }

    async fn validate_source_table_id(
        &self,
        physical_query: &str,
        expected_source_table_id: u64,
    ) -> Result<()> {
        let query = parse_materialized_view_query(
            physical_query,
            "invalid materialized view physical query",
        )?;
        let mut planner = Planner::new_with_query_executor(
            self.ctx.clone(),
            Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
                self.ctx.as_ref(),
            ))),
        );
        let plan = planner
            .plan_stmt(&Statement::Query(Box::new(query)), false)
            .await
            .map_err(|error| {
                ErrorCode::InvalidMaterializedView(format!(
                    "materialized view {}.{} source table changed: expected table id {}: {}",
                    self.plan.database, self.plan.view_name, expected_source_table_id, error
                ))
            })?;
        let databend_common_sql::plans::Plan::Query { metadata, .. } = plan else {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {}.{} physical definition is not a query",
                self.plan.database, self.plan.view_name
            )));
        };
        validate_materialized_view_source(
            &metadata,
            expected_source_table_id,
            &format!("{}.{}", self.plan.database, self.plan.view_name),
        )
    }

    fn attach_source(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        source: Arc<dyn Table>,
    ) -> Result<()> {
        self.ctx.evict_table_from_cache(catalog, database, table)?;
        self.ctx.attach_table(catalog, database, table, source);
        Ok(())
    }

    async fn update_compaction_options(
        &self,
        table: &dyn Table,
        base_blocks: u64,
        delta_blocks: u64,
    ) -> Result<()> {
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        catalog
            .upsert_table_option(
                &self.plan.tenant,
                &self.plan.database,
                UpsertTableOptionReq {
                    table_id: table.get_id(),
                    seq: MatchSeq::Exact(table.get_table_info().ident.seq),
                    options: HashMap::from([
                        (
                            OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_BASE_BLOCKS.to_string(),
                            Some(base_blocks.to_string()),
                        ),
                        (
                            OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_DELTA_BLOCKS.to_string(),
                            Some(delta_blocks.to_string()),
                        ),
                    ]),
                },
            )
            .await?;
        Ok(())
    }

    async fn maintain_aggregate_compaction(
        &self,
        effect: AggregateRefreshEffect,
        previous_block_count: u64,
    ) -> Result<()> {
        if effect == AggregateRefreshEffect::None {
            return Ok(());
        }
        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.view_name,
        )?;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let table = catalog
            .get_table(&self.plan.tenant, &self.plan.database, &self.plan.view_name)
            .await?;
        if table.get_id() != self.mv_table.get_id() || table.engine() != MATERIALIZED_VIEW_ENGINE {
            return Ok(());
        }
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let current_block_count = fuse_table
            .read_table_snapshot()
            .await?
            .map(|snapshot| snapshot.summary.block_count)
            .unwrap_or(0);

        // A rebuild already emits one globally merged state per group, so it establishes a new
        // compaction baseline. Only append-only aggregate refreshes accumulate semantic debt.
        if effect == AggregateRefreshEffect::Rebuilt {
            self.update_compaction_options(table.as_ref(), current_block_count, 0)
                .await?;
            return Ok(());
        }

        let options = table.options();
        let base_blocks = options
            .get(OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_BASE_BLOCKS)
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|error| {
                ErrorCode::InvalidMaterializedView(format!(
                    "invalid aggregate MV compaction base block count: {error}"
                ))
            })?
            .unwrap_or(previous_block_count);
        let previous_delta_blocks = options
            .get(OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_DELTA_BLOCKS)
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|error| {
                ErrorCode::InvalidMaterializedView(format!(
                    "invalid aggregate MV compaction delta block count: {error}"
                ))
            })?
            .unwrap_or(0);
        let delta_blocks = previous_delta_blocks
            .saturating_add(current_block_count.saturating_sub(previous_block_count));
        // Persist debt before the best-effort overwrite. A failed compaction must leave the debt
        // visible so a later append-only refresh can retry instead of treating it as compacted.
        self.update_compaction_options(table.as_ref(), base_blocks, delta_blocks)
            .await?;

        let compact_threshold = AGGREGATE_MV_COMPACT_MIN_DELTA_BLOCKS.max(base_blocks / 10);
        if delta_blocks < compact_threshold || !self.compact_aggregate_states().await? {
            return Ok(());
        }

        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.view_name,
        )?;
        let compacted_table = catalog
            .get_table(&self.plan.tenant, &self.plan.database, &self.plan.view_name)
            .await?;
        if compacted_table.get_id() != self.mv_table.get_id()
            || compacted_table.engine() != MATERIALIZED_VIEW_ENGINE
        {
            return Ok(());
        }
        let compacted_fuse = FuseTable::try_from_table(compacted_table.as_ref())?;
        let compacted_block_count = compacted_fuse
            .read_table_snapshot()
            .await?
            .map(|snapshot| snapshot.summary.block_count)
            .unwrap_or(0);
        self.update_compaction_options(compacted_table.as_ref(), compacted_block_count, 0)
            .await
    }

    async fn compact_aggregate_states(&self) -> Result<bool> {
        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.view_name,
        )?;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let mv_table = catalog
            .get_table(&self.plan.tenant, &self.plan.database, &self.plan.view_name)
            .await?;
        if mv_table.get_id() != self.mv_table.get_id()
            || mv_table.engine() != MATERIALIZED_VIEW_ENGINE
        {
            return Ok(false);
        }
        let fuse_table = FuseTable::try_from_table(mv_table.as_ref())?;
        let Some(_snapshot) = fuse_table.read_table_snapshot().await? else {
            return Ok(false);
        };

        let physical_source_name = format!(
            "_mv_compact_{}_{}",
            mv_table.get_id(),
            mv_table.get_table_info().ident.seq
        );
        let mut physical_source_info = mv_table.get_table_info().clone();
        physical_source_info.name = physical_source_name.clone();
        physical_source_info.desc = format!("'{}'.'{}'", self.plan.database, physical_source_name);
        // Build a query-local FUSE alias over the committed MV snapshot so compaction reads the
        // persisted AggregateState columns directly. Keeping the MATERIALIZED VIEW engine would
        // route the alias through bind_materialized_view(), which applies the logical read plan and
        // finalizes states with *_merge before this compaction can preserve them with *_merge_state.
        // This only changes the cloned TableInfo; the catalog entry remains a MATERIALIZED VIEW.
        physical_source_info.meta.engine = "FUSE".to_string();
        let physical_source = catalog.get_table_by_info(&physical_source_info)?;
        self.attach_source(
            &self.plan.catalog,
            &self.plan.database,
            &physical_source_name,
            physical_source,
        )?;

        let mut state_targets = Vec::new();
        let mut group_targets = Vec::new();
        let mut saw_group = false;
        for field in mv_table.schema().fields() {
            let column = Identifier::from_name(None, field.name()).to_string();
            match field.data_type().remove_nullable() {
                TableDataType::AggregateState { function_name, .. } => {
                    if saw_group {
                        return Err(ErrorCode::InvalidMaterializedView(
                            "aggregate state columns must precede GROUP BY columns in materialized view storage",
                        ));
                    }
                    state_targets.push(format!(
                        "{}_merge_state({column}) AS {column}",
                        function_name
                    ));
                }
                _ => {
                    saw_group = true;
                    group_targets.push(column);
                }
            }
        }
        if state_targets.is_empty() && group_targets.is_empty() {
            return Ok(false);
        }

        let mut targets = state_targets;
        targets.extend(group_targets.iter().cloned());
        let source = format!(
            "{}.{}.{}",
            Identifier::from_name(None, &self.plan.catalog),
            Identifier::from_name(None, &self.plan.database),
            Identifier::from_name(None, &physical_source_name),
        );
        let mut sql = format!("SELECT {} FROM {source}", targets.join(", "));
        if !group_targets.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_targets.join(", ")));
        }
        let query = parse_materialized_view_query(
            &sql,
            "invalid materialized view aggregate compact query",
        )?;
        self.execute_statement(&Statement::Insert(InsertStmt {
            hints: None,
            with: None,
            table: TableRef {
                catalog: Some(Identifier::from_name(None, &self.plan.catalog)),
                database: Some(Identifier::from_name(None, &self.plan.database)),
                table: Identifier::from_name(None, &self.plan.view_name),
                branch: None,
            },
            columns: vec![],
            source: InsertSource::Select {
                query: Box::new(query),
            },
            overwrite: true,
        }))
        .await?;
        Ok(true)
    }

    async fn execute_statement(&self, statement: &Statement) -> Result<()> {
        let mut planner = Planner::new_with_query_executor(
            self.ctx.clone(),
            Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
                self.ctx.as_ref(),
            ))),
        );
        let plan = planner.plan_stmt(statement, false).await?;
        let interpreter = match plan {
            databend_common_sql::plans::Plan::Insert(insert) => {
                InsertInterpreter::try_create_materialized_view_refresh(
                    self.ctx.clone(),
                    *insert,
                    self.mv_table.get_id(),
                )?
            }
            databend_common_sql::plans::Plan::DataMutation { s_expr, schema, .. } => {
                let mutation: databend_common_sql::plans::Mutation =
                    s_expr.plan().clone().try_into()?;
                Arc::new(MutationInterpreter::try_create_materialized_view_refresh(
                    self.ctx.clone(),
                    *s_expr,
                    schema,
                    mutation.metadata,
                    self.mv_table.get_id(),
                )?)
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "materialized view refresh produced an unsupported internal statement",
                ));
            }
        };
        let stream = interpreter
            .execute_with_hooks(self.ctx.clone(), QueryFinishHooks::nested_with_hooks())
            .await?;
        futures::pin_mut!(stream);
        use futures::TryStreamExt;
        while stream.try_next().await?.is_some() {}
        Ok(())
    }

    fn column_ref(table: Option<&str>, column: &str) -> Expr {
        Expr::ColumnRef {
            span: None,
            column: ColumnRef {
                database: None,
                table: table.map(|name| Identifier::from_name(None, name)),
                column: ColumnID::Name(Identifier::from_name(None, column)),
            },
        }
    }

    fn string_literal(value: &str) -> Expr {
        Expr::Literal {
            span: None,
            value: Literal::String(value.to_string()),
        }
    }

    fn apply_changes_query(
        query: &mut Query,
        changes_query: Query,
        change_action: &str,
    ) -> Result<()> {
        let SetExpr::Select(select) = &mut query.body else {
            return Err(ErrorCode::Internal(
                "materialized view physical query must be a SELECT",
            ));
        };
        let [source] = select.from.as_mut_slice() else {
            return Err(ErrorCode::Internal(
                "materialized view refresh requires exactly one base table",
            ));
        };
        let alias = match source {
            TableReference::Table { alias, .. } => alias.clone(),
            _ => None,
        };
        *source = TableReference::Subquery {
            span: None,
            lateral: false,
            subquery: Box::new(changes_query),
            alias,
            pivot: None,
            unpivot: None,
        };

        let insert_filter = Expr::BinaryOp {
            span: None,
            op: BinaryOperator::Eq,
            left: Box::new(Self::column_ref(None, "change$action")),
            right: Box::new(Self::string_literal(change_action)),
        };
        select.selection = Some(match select.selection.take() {
            Some(filter) => Expr::BinaryOp {
                span: None,
                op: BinaryOperator::And,
                left: Box::new(filter),
                right: Box::new(insert_filter),
            },
            None => insert_filter,
        });
        Ok(())
    }

    fn build_standard_refresh_source(
        &self,
        physical_query: &str,
        changes_query: &Query,
    ) -> Result<Query> {
        let mut upserts = parse_materialized_view_query(
            physical_query,
            "invalid materialized view physical query",
        )?;
        Self::apply_changes_query(&mut upserts, changes_query.clone(), "INSERT")?;

        let mut deletes = parse_materialized_view_query(
            physical_query,
            "invalid materialized view physical query",
        )?;
        Self::apply_changes_query(&mut deletes, changes_query.clone(), "DELETE")?;

        let row_id = Identifier::from_name(None, MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN);
        parse_materialized_view_query(
            &format!(
                "WITH upserts AS ({upserts}), deletes AS ({deletes}) \
                 SELECT 'UPSERT' AS _mv_refresh_action, upserts.* FROM upserts \
                 UNION ALL \
                 SELECT 'DELETE' AS _mv_refresh_action, deletes.* FROM deletes \
                 WHERE NOT EXISTS (SELECT 1 FROM upserts \
                 WHERE upserts.{row_id} = deletes.{row_id})"
            ),
            "invalid materialized view standard refresh query",
        )
    }

    fn build_standard_refresh_merge(&self, source: Query) -> Statement {
        let schema = self.mv_table.schema();
        let fields = schema.fields();
        let update_list = fields
            .iter()
            .map(|field| MutationUpdateExpr {
                table: Some(Identifier::from_name(None, "mv")),
                name: Identifier::from_name(None, field.name()),
                expr: Self::column_ref(Some("changes"), field.name()),
            })
            .collect();
        let columns = fields
            .iter()
            .map(|field| Identifier::from_name(None, field.name()))
            .collect();
        let values = fields
            .iter()
            .map(|field| Self::column_ref(Some("changes"), field.name()))
            .collect();
        let action_is = |action| Expr::BinaryOp {
            span: None,
            op: BinaryOperator::Eq,
            left: Box::new(Self::column_ref(Some("changes"), "_mv_refresh_action")),
            right: Box::new(Self::string_literal(action)),
        };

        Statement::MergeInto(MergeIntoStmt {
            hints: None,
            catalog: Some(Identifier::from_name(None, &self.plan.catalog)),
            database: Some(Identifier::from_name(None, &self.plan.database)),
            table_ident: Identifier::from_name(None, &self.plan.view_name),
            source: Self::changes_source(source),
            target_alias: Some(TableAlias {
                name: Identifier::from_name(None, "mv"),
                columns: vec![],
                keep_database_name: false,
            }),
            join_expr: Expr::BinaryOp {
                span: None,
                op: BinaryOperator::Eq,
                left: Box::new(Self::column_ref(
                    Some("mv"),
                    MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN,
                )),
                right: Box::new(Self::column_ref(
                    Some("changes"),
                    MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN,
                )),
            },
            merge_options: vec![
                MergeOption::Match(MatchedClause {
                    selection: Some(action_is("DELETE")),
                    operation: MatchOperation::Delete,
                }),
                MergeOption::Match(MatchedClause {
                    selection: Some(action_is("UPSERT")),
                    operation: MatchOperation::Update {
                        update_list,
                        is_star: false,
                    },
                }),
                MergeOption::Unmatch(UnmatchedClause {
                    selection: Some(action_is("UPSERT")),
                    insert_operation: InsertOperation {
                        columns: Some(columns),
                        values,
                        is_star: false,
                    },
                }),
            ],
        })
    }

    fn changes_source(changes_query: Query) -> MutationSource {
        MutationSource::Select {
            query: Box::new(changes_query),
            source_alias: TableAlias {
                name: Identifier::from_name(None, "changes"),
                columns: vec![],
                keep_database_name: false,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_in_transaction(
        &self,
        source_table: &FuseTable,
        physical_query: &str,
        source_database: &str,
        source_table_name: &str,
        is_aggregating: bool,
        previous_mv_block_count: u64,
        checkpoint: Option<(u64, Option<String>)>,
        source_seq: u64,
        end_snapshot: Option<String>,
    ) -> Result<()> {
        let mut aggregate_refresh_effect = AggregateRefreshEffect::None;
        if end_snapshot.is_none() {
            // A source without a snapshot is a valid empty endpoint. Rebind the physical query to
            // the captured empty table and overwrite the MV so old materialized rows are removed.
            self.attach_source(
                &self.plan.catalog,
                source_database,
                source_table_name,
                Arc::new(source_table.clone()),
            )?;
            let query = parse_materialized_view_query(
                physical_query,
                "invalid materialized view physical query",
            )?;
            self.execute_statement(&Statement::Insert(InsertStmt {
                hints: None,
                with: None,
                table: TableRef {
                    catalog: Some(Identifier::from_name(None, &self.plan.catalog)),
                    database: Some(Identifier::from_name(None, &self.plan.database)),
                    table: Identifier::from_name(None, &self.plan.view_name),
                    branch: None,
                },
                columns: vec![],
                source: InsertSource::Select {
                    query: Box::new(query),
                },
                overwrite: true,
            }))
            .await?;
            if is_aggregating {
                aggregate_refresh_effect = AggregateRefreshEffect::Rebuilt;
            }
        } else if checkpoint
            .as_ref()
            .is_none_or(|(_, start_snapshot)| start_snapshot.is_none())
        {
            let changes_source_name = format!("_mv_changes_{}_0", self.mv_table.get_id());
            let changes = source_table
                .get_changes_query(
                    self.ctx.clone(),
                    &StreamMode::Standard,
                    &None,
                    format!(
                        "{}.{}",
                        Identifier::from_name(None, source_database),
                        Identifier::from_name(None, &changes_source_name)
                    ),
                    0,
                )
                .await?;
            let changes_source_table = source_table.with_changes_desc(ChangesDesc {
                mode: changes.mode,
                seq: 0,
                location: None,
                desc: String::new(),
            });
            self.attach_source(
                &self.plan.catalog,
                source_database,
                &changes_source_name,
                Arc::new(changes_source_table),
            )?;
            let changes_query =
                parse_materialized_view_query(&changes.query, "invalid CHANGE_TRACKING query")?;
            let mut query = parse_materialized_view_query(
                physical_query,
                "invalid materialized view physical query",
            )?;
            Self::apply_changes_query(&mut query, changes_query, "INSERT")?;
            self.execute_statement(&Statement::Insert(InsertStmt {
                hints: None,
                with: None,
                table: TableRef {
                    catalog: Some(Identifier::from_name(None, &self.plan.catalog)),
                    database: Some(Identifier::from_name(None, &self.plan.database)),
                    table: Identifier::from_name(None, &self.plan.view_name),
                    branch: None,
                },
                columns: vec![],
                source: InsertSource::Select {
                    query: Box::new(query),
                },
                overwrite: true,
            }))
            .await?;
            if is_aggregating {
                aggregate_refresh_effect = AggregateRefreshEffect::Rebuilt;
            }
        } else if let Some((mv_source_seq, Some(start_snapshot))) = checkpoint
            && mv_source_seq != source_seq
        {
            let changes_source_name =
                format!("_mv_changes_{}_{}", self.mv_table.get_id(), mv_source_seq);
            let changes = source_table
                .get_changes_query(
                    self.ctx.clone(),
                    &StreamMode::Standard,
                    &Some(start_snapshot.clone()),
                    format!(
                        "{}.{}",
                        Identifier::from_name(None, source_database),
                        Identifier::from_name(None, &changes_source_name)
                    ),
                    mv_source_seq,
                )
                .await?;
            let changes_source_table = source_table.with_changes_desc(ChangesDesc {
                mode: changes.mode.clone(),
                seq: mv_source_seq,
                location: Some(start_snapshot),
                desc: String::new(),
            });
            self.attach_source(
                &self.plan.catalog,
                source_database,
                &changes_source_name,
                Arc::new(changes_source_table),
            )?;
            let changes_query =
                parse_materialized_view_query(&changes.query, "invalid CHANGE_TRACKING query")?;
            if changes.mode == StreamMode::Standard && is_aggregating {
                self.attach_source(
                    &self.plan.catalog,
                    source_database,
                    source_table_name,
                    Arc::new(source_table.clone()),
                )?;
                let query = parse_materialized_view_query(
                    physical_query,
                    "invalid materialized view physical query",
                )?;
                self.execute_statement(&Statement::Insert(InsertStmt {
                    hints: None,
                    with: None,
                    table: TableRef {
                        catalog: Some(Identifier::from_name(None, &self.plan.catalog)),
                        database: Some(Identifier::from_name(None, &self.plan.database)),
                        table: Identifier::from_name(None, &self.plan.view_name),
                        branch: None,
                    },
                    columns: vec![],
                    source: InsertSource::Select {
                        query: Box::new(query),
                    },
                    overwrite: true,
                }))
                .await?;
                aggregate_refresh_effect = AggregateRefreshEffect::Rebuilt;
            } else if changes.mode == StreamMode::Standard {
                let refresh_source =
                    self.build_standard_refresh_source(physical_query, &changes_query)?;
                let merge = self.build_standard_refresh_merge(refresh_source);
                self.execute_statement(&merge).await?;
            } else {
                let mut query = parse_materialized_view_query(
                    physical_query,
                    "invalid materialized view physical query",
                )?;
                Self::apply_changes_query(&mut query, changes_query, "INSERT")?;
                self.execute_statement(&Statement::Insert(InsertStmt {
                    hints: None,
                    with: None,
                    table: TableRef {
                        catalog: Some(Identifier::from_name(None, &self.plan.catalog)),
                        database: Some(Identifier::from_name(None, &self.plan.database)),
                        table: Identifier::from_name(None, &self.plan.view_name),
                        branch: None,
                    },
                    columns: vec![],
                    source: InsertSource::Select {
                        query: Box::new(query),
                    },
                    overwrite: false,
                }))
                .await?;
                if is_aggregating {
                    aggregate_refresh_effect = AggregateRefreshEffect::Appended;
                }
            }
        }

        let checkpoint_options = HashMap::from([
            (
                OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ.to_string(),
                Some(source_seq.to_string()),
            ),
            (
                OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION.to_string(),
                end_snapshot,
            ),
        ]);
        let txn_mgr = self.ctx.txn_mgr();
        let updated = txn_mgr
            .lock()
            .update_table_options(self.mv_table.get_id(), checkpoint_options.clone());
        if !updated {
            let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
            catalog
                .upsert_table_option(
                    &self.plan.tenant,
                    &self.plan.database,
                    UpsertTableOptionReq {
                        table_id: self.mv_table.get_id(),
                        seq: MatchSeq::Exact(self.mv_table.get_table_info().ident.seq),
                        options: checkpoint_options,
                    },
                )
                .await?;
        }
        execute_commit_statement(self.ctx.clone()).await?;

        if is_aggregating {
            if let Err(error) = self
                .maintain_aggregate_compaction(aggregate_refresh_effect, previous_mv_block_count)
                .await
            {
                warn!(
                    "materialized view {}.{} refresh succeeded but aggregate state compaction maintenance failed: {}",
                    self.plan.database, self.plan.view_name, error
                );
            }
        }
        Ok(())
    }
}
