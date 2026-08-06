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
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_ENGINE;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_sql::MaterializedViewChecker;
use databend_common_sql::Planner;
use databend_common_sql::parse_materialized_view_query;
use databend_common_sql::plans::Mutation;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RefreshMaterializedViewPlan;
use databend_common_sql::validate_materialized_view_source;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::ChangesDesc;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
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
// have appended enough state blocks.
const AGGREGATE_MV_COMPACT_MIN_DELTA_BLOCKS: u64 = 32;

#[derive(Clone, Copy, Eq, PartialEq)]
enum AggregateRefreshEffect {
    None,
    Rebuilt,
    Appended,
}

enum RefreshStrategy {
    CheckpointOnly,
    Rebuild(Statement),
    Merge(Statement),
    Append(Statement),
    AppendAggregate {
        statement: Statement,
        appended_blocks: u64,
    },
}

impl RefreshStrategy {
    fn aggregate_effect(&self, is_aggregating: bool) -> AggregateRefreshEffect {
        if !is_aggregating {
            return AggregateRefreshEffect::None;
        }
        match self {
            RefreshStrategy::Rebuild(_) => AggregateRefreshEffect::Rebuilt,
            RefreshStrategy::AppendAggregate { .. } => AggregateRefreshEffect::Appended,
            RefreshStrategy::CheckpointOnly
            | RefreshStrategy::Merge(_)
            | RefreshStrategy::Append(_) => AggregateRefreshEffect::None,
        }
    }

    fn statement(&self) -> Option<&Statement> {
        match self {
            RefreshStrategy::CheckpointOnly => None,
            RefreshStrategy::Rebuild(statement)
            | RefreshStrategy::Merge(statement)
            | RefreshStrategy::Append(statement) => Some(statement),
            RefreshStrategy::AppendAggregate { statement, .. } => Some(statement),
        }
    }

    fn appended_blocks(&self) -> Option<u64> {
        match self {
            RefreshStrategy::AppendAggregate {
                appended_blocks, ..
            } => Some(*appended_blocks),
            _ => None,
        }
    }
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
        if let Some(refresh) =
            MaterializedViewRefresh::create(table, self.ctx.clone(), &self.plan).await?
        {
            refresh.execute().await?;
        }
        Ok(PipelineBuildResult::create())
    }
}

struct MaterializedViewRefresh<'a> {
    mv_table: &'a FuseTable,
    ctx: Arc<QueryContext>,
    plan: &'a RefreshMaterializedViewPlan,
    source_table: FuseTable,
    physical_query: String,
    source_database: String,
    source_table_name: String,
    is_aggregating: bool,
    previous_mv_block_count: u64,
    checkpoint: Option<(u64, Option<String>)>,
    source_seq: u64,
    end_snapshot: Option<String>,
}

impl<'a> MaterializedViewRefresh<'a> {
    async fn create(
        mv_table: &'a FuseTable,
        ctx: Arc<QueryContext>,
        plan: &'a RefreshMaterializedViewPlan,
    ) -> Result<Option<Self>> {
        let mv_meta = &mv_table.get_table_info().meta;
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

        let catalog = ctx.get_catalog(&plan.catalog).await?;
        // Binding validity is an admission check, not a refresh fence. Meta reads the definition
        // and both generations at one transaction point. Once admitted, a concurrent source-schema
        // change may let this refresh finish; subsequent MV reads reject the stale binding.
        let definition = catalog
            .get_active_mv_definition(&plan.tenant, source_table_id, mv_table.get_id())
            .await?
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(format!(
                    "materialized view {}.{} has an invalid source binding; recreate the materialized view",
                    plan.database, plan.view_name
                ))
            })?;
        Self::validate_source_table_id(ctx.clone(), plan, &definition.data.query, source_table_id)
            .await?;
        let source_meta = catalog
            .get_table_meta_by_id(source_table_id)
            .await?
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(format!(
                    "materialized view {}.{} source table changed: expected table id {} no longer exists",
                    plan.database, plan.view_name, source_table_id
                ))
            })?;
        if source_meta.data.drop_on.is_some() {
            return Err(ErrorCode::UnknownTable(format!(
                "materialized view {}.{} source table id {} has been dropped",
                plan.database, plan.view_name, source_table_id
            )));
        }
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
                plan.database, plan.view_name, source_table_id
            )));
        }
        if checkpoint
            .as_ref()
            .is_some_and(|(mv_source_seq, _)| *mv_source_seq > source_seq)
        {
            let mv_source_seq = checkpoint.as_ref().unwrap().0;
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {}.{} offset {} is newer than source table version {}",
                plan.database, plan.view_name, mv_source_seq, source_seq
            )));
        }

        if checkpoint.as_ref() == Some(&(source_seq, source_snapshot_location.clone())) {
            return Ok(None);
        }

        info!(
            "materialized view {}.{} refresh offsets: source_table_id={}, mv_offset={:?}, source_offset={}, initial_refresh={}, changed={}",
            plan.database,
            plan.view_name,
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
        let source_table_info = TableInfo::new_full(
            &source_database,
            &source_table_name,
            TableIdent::new(source_table_id, source_seq),
            source_meta.data.clone(),
            catalog.info(),
            DatabaseType::NormalDB,
        );
        let source_table = catalog.get_table_by_info(&source_table_info)?;
        let source_table = FuseTable::try_from_table(source_table.as_ref())?;
        if let Some((mv_source_seq, Some(_))) = &checkpoint {
            source_table
                .check_changes_valid(&source_table.get_table_info().desc, *mv_source_seq)?;
        }

        let previous_mv_block_count = mv_table
            .read_table_snapshot()
            .await?
            .map(|snapshot| snapshot.summary.block_count)
            .unwrap_or(0);
        Ok(Some(Self {
            mv_table,
            ctx,
            plan,
            source_table: source_table.clone(),
            physical_query: definition.data.query.clone(),
            source_database,
            source_table_name,
            is_aggregating,
            previous_mv_block_count,
            checkpoint,
            source_seq,
            end_snapshot: source_snapshot_location,
        }))
    }

    async fn validate_source_table_id(
        ctx: Arc<QueryContext>,
        refresh_plan: &RefreshMaterializedViewPlan,
        physical_query: &str,
        expected_source_table_id: u64,
    ) -> Result<()> {
        let query = parse_materialized_view_query(
            physical_query,
            "invalid materialized view physical query",
        )?;
        let mut planner = Planner::new_with_query_executor(
            ctx.clone(),
            Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
                ctx.as_ref(),
            ))),
        );
        let plan = planner
            .plan_stmt(&Statement::Query(Box::new(query)), false)
            .await
            .map_err(|error| {
                ErrorCode::InvalidMaterializedView(format!(
                    "materialized view {}.{} source table changed: expected table id {}: {}",
                    refresh_plan.database, refresh_plan.view_name, expected_source_table_id, error
                ))
            })?;
        let databend_common_sql::plans::Plan::Query { metadata, .. } = plan else {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {}.{} physical definition is not a query",
                refresh_plan.database, refresh_plan.view_name
            )));
        };
        validate_materialized_view_source(
            &metadata,
            expected_source_table_id,
            &format!("{}.{}", refresh_plan.database, refresh_plan.view_name),
        )
    }

    /// Bind a qualified table name to an exact `Table` instance for this query context.
    ///
    /// MV refresh uses this to pin internal CHANGE_TRACKING scans and full rebuilds to the source
    /// endpoint captured during refresh initialization. Later source commits must not make a
    /// refresh read newer data while it still records the captured checkpoint. Evicting first also
    /// replaces any table instance cached earlier in the same query context.
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

    async fn compact_aggregate_states(&self, mv_table: &dyn Table) -> Result<bool> {
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let fuse_table = FuseTable::try_from_table(mv_table)?;
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
            Plan::Insert(insert) => InsertInterpreter::try_create_materialized_view_refresh(
                self.ctx.clone(),
                *insert,
                self.mv_table.get_id(),
            )?,
            Plan::DataMutation { s_expr, schema, .. } => {
                let mutation: Mutation = s_expr.plan().clone().try_into()?;
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

    fn target_insert(&self, query: Query, overwrite: bool) -> Statement {
        Statement::Insert(InsertStmt {
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
            overwrite,
        })
    }

    fn rebuild_strategy(&self, physical_query: &str) -> Result<RefreshStrategy> {
        let query = parse_materialized_view_query(
            physical_query,
            "invalid materialized view physical query",
        )?;
        let insert_stmt = self.target_insert(query, true);
        Ok(RefreshStrategy::Rebuild(insert_stmt))
    }

    async fn plan_refresh_strategy(&self) -> Result<RefreshStrategy> {
        let source_table = &self.source_table;
        let physical_query = &self.physical_query;
        let source_database = &self.source_database;
        let source_table_name = &self.source_table_name;
        let checkpoint = &self.checkpoint;
        let end_snapshot = &self.end_snapshot;
        // 1. An empty source has no snapshot. Overwrite the MV to remove any previously persisted rows.
        if end_snapshot.is_none() {
            self.attach_source(
                &self.plan.catalog,
                source_database,
                source_table_name,
                Arc::new(source_table.clone()),
            )?;
            return self.rebuild_strategy(physical_query);
        }

        // 2. The data endpoint is unchanged. No storage write is needed; only advance metadata if its
        // source sequence changed independently of the snapshot.
        if checkpoint
            .as_ref()
            .is_some_and(|(_, start_snapshot)| start_snapshot == end_snapshot)
        {
            return Ok(RefreshStrategy::CheckpointOnly);
        }

        let (checkpoint_seq, start_snapshot) = match checkpoint.clone() {
            Some((checkpoint_seq, Some(start_snapshot))) => (checkpoint_seq, Some(start_snapshot)),
            None | Some((_, None)) => (0, None),
        };
        let starts_from_empty_endpoint = start_snapshot.is_none();
        let changes_source_name =
            format!("_mv_changes_{}_{}", self.mv_table.get_id(), checkpoint_seq);
        let changes = source_table
            .get_changes_query(
                self.ctx.clone(),
                &StreamMode::Standard,
                &start_snapshot,
                format!(
                    "{}.{}",
                    Identifier::from_name(None, source_database),
                    Identifier::from_name(None, &changes_source_name)
                ),
                checkpoint_seq,
            )
            .await?;

        // 3. Aggregate states cannot retract UPDATE/DELETE effects. Recompute globally merged states
        // from the current source and replace all persisted state rows.
        if !starts_from_empty_endpoint
            && self.is_aggregating
            && changes.mode == StreamMode::Standard
        {
            self.attach_source(
                &self.plan.catalog,
                source_database,
                source_table_name,
                Arc::new(source_table.clone()),
            )?;
            return self.rebuild_strategy(physical_query);
        }

        let changes_source_table = source_table.with_changes_desc(ChangesDesc {
            mode: changes.mode.clone(),
            seq: checkpoint_seq,
            location: start_snapshot,
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
        Self::apply_changes_query(&mut query, changes_query.clone(), "INSERT")?;
        // 4. The first refresh consumes all tracked inserts with INSERT OVERWRITE. For aggregate MVs,
        // this also establishes a globally merged baseline with no semantic compaction debt.
        if starts_from_empty_endpoint {
            return Ok(RefreshStrategy::Rebuild(self.target_insert(query, true)));
        }

        // 5. Standard non-aggregate changes apply UPDATE/DELETE/INSERT through one internal MERGE.
        if changes.mode == StreamMode::Standard {
            let source = self.build_standard_refresh_source(physical_query, &changes_query)?;
            return Ok(RefreshStrategy::Merge(
                self.build_standard_refresh_merge(source),
            ));
        }

        // 6. AppendOnly changes can be appended directly. Aggregate appends persist additional state
        // rows and are the only refresh strategy that accumulates semantic compaction debt.
        let statement = self.target_insert(query, false);
        if self.is_aggregating {
            Ok(RefreshStrategy::AppendAggregate {
                statement,
                appended_blocks: 0,
            })
        } else {
            Ok(RefreshStrategy::Append(statement))
        }
    }

    fn parse_compaction_debt(&self) -> Result<u64> {
        self.mv_table
            .options()
            .get(OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_DELTA_BLOCKS)
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|error| {
                ErrorCode::InvalidMaterializedView(format!(
                    "invalid aggregate MV compaction delta block count: {error}"
                ))
            })
            .map(|value| value.unwrap_or(0))
    }

    async fn transaction_local_mv_block_count(&self) -> Result<u64> {
        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.view_name,
        )?;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let table = catalog
            .get_table(&self.plan.tenant, &self.plan.database, &self.plan.view_name)
            .await?;
        FuseTable::try_from_table(table.as_ref())?
            .read_table_snapshot()
            .await
            .map(|snapshot| {
                snapshot
                    .map(|snapshot| snapshot.summary.block_count)
                    .unwrap_or(0)
            })
    }

    async fn execute_planned_refresh(&self, strategy: &mut RefreshStrategy) -> Result<()> {
        let Some(statement) = strategy.statement() else {
            return Ok(());
        };
        self.execute_statement(statement).await?;
        if let RefreshStrategy::AppendAggregate {
            appended_blocks, ..
        } = strategy
        {
            let current_mv_block_count = self.transaction_local_mv_block_count().await?;

            *appended_blocks = current_mv_block_count.saturating_sub(self.previous_mv_block_count);
        }
        Ok(())
    }

    fn checkpoint_options(
        &self,
        strategy: &RefreshStrategy,
        aggregate_effect: AggregateRefreshEffect,
    ) -> Result<HashMap<String, Option<String>>> {
        let compaction_debt = match aggregate_effect {
            AggregateRefreshEffect::None => None,
            AggregateRefreshEffect::Rebuilt => Some(0),
            AggregateRefreshEffect::Appended => {
                let appended_blocks = strategy.appended_blocks().ok_or_else(|| {
                    ErrorCode::Internal("aggregate append refresh did not record appended blocks")
                })?;
                Some(
                    self.parse_compaction_debt()?
                        .saturating_add(appended_blocks),
                )
            }
        };

        let source_seq = (
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ.to_string(),
            Some(self.source_seq.to_string()),
        );
        let source_snapshot = (
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION.to_string(),
            self.end_snapshot.clone(),
        );

        let options = match compaction_debt {
            Some(delta_blocks) => HashMap::from([
                source_seq,
                source_snapshot,
                (
                    OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_DELTA_BLOCKS.to_string(),
                    Some(delta_blocks.to_string()),
                ),
            ]),
            None => HashMap::from([source_seq, source_snapshot]),
        };
        Ok(options)
    }

    async fn execute_refresh(&self) -> Result<AggregateRefreshEffect> {
        let mut strategy = self.plan_refresh_strategy().await?;
        self.execute_planned_refresh(&mut strategy).await?;
        let aggregate_effect = strategy.aggregate_effect(self.is_aggregating);
        let checkpoint_options = self.checkpoint_options(&strategy, aggregate_effect)?;

        let txn_mgr = self.ctx.txn_mgr();
        let updated = txn_mgr
            .lock()
            .update_table_options(self.mv_table.get_id(), checkpoint_options.clone());
        if !updated && aggregate_effect != AggregateRefreshEffect::None {
            return Err(ErrorCode::Internal(format!(
                "aggregate materialized view {}.{} refresh did not buffer its table mutation",
                self.plan.database, self.plan.view_name
            )));
        }
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
        Ok(aggregate_effect)
    }

    async fn compact_after_refresh(&self, aggregate_effect: AggregateRefreshEffect) -> Result<()> {
        if aggregate_effect != AggregateRefreshEffect::Appended {
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
        if table.get_id() != self.mv_table.get_id() {
            return Ok(());
        }
        let delta_blocks = table
            .options()
            .get(OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_DELTA_BLOCKS)
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|error| {
                ErrorCode::InvalidMaterializedView(format!(
                    "invalid aggregate MV compaction delta block count: {error}"
                ))
            })?
            .unwrap_or(0);
        if delta_blocks < AGGREGATE_MV_COMPACT_MIN_DELTA_BLOCKS
            || !self.compact_aggregate_states(table.as_ref()).await?
        {
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
        if compacted_table.get_id() != self.mv_table.get_id() {
            return Ok(());
        }
        catalog
            .upsert_table_option(
                &self.plan.tenant,
                &self.plan.database,
                UpsertTableOptionReq {
                    table_id: compacted_table.get_id(),
                    seq: MatchSeq::Exact(compacted_table.get_table_info().ident.seq),
                    options: HashMap::from([(
                        OPT_KEY_MATERIALIZED_VIEW_AGGREGATE_COMPACTION_DELTA_BLOCKS.to_string(),
                        Some("0".to_string()),
                    )]),
                },
            )
            .await?;
        Ok(())
    }

    async fn execute(&self) -> Result<()> {
        let txn_mgr = self.ctx.txn_mgr();
        if txn_mgr.lock().is_active() {
            return Err(ErrorCode::InvalidOperation(
                "REFRESH MATERIALIZED VIEW cannot run inside an active transaction",
            ));
        }
        txn_mgr.lock().begin();

        let aggregate_effect = match self.execute_refresh().await {
            Ok(effect) => effect,
            Err(error) => {
                // execute_commit_statement() clears the transaction itself once entered. This
                // explicit clear covers planning and refresh failures before commit starts.
                txn_mgr.lock().clear();
                return Err(error);
            }
        };

        // Refresh data, checkpoint, and debt are committed at this point. Semantic compaction is a
        // separate best-effort maintenance phase and cannot invalidate the completed refresh.
        if let Err(error) = self.compact_after_refresh(aggregate_effect).await {
            warn!(
                "materialized view {}.{} refresh succeeded but aggregate state compaction maintenance failed: {}",
                self.plan.database, self.plan.view_name, error
            );
        }
        Ok(())
    }
}
