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

use std::collections::BTreeSet;
use std::sync::Arc;

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnSet;
use databend_common_sql::MetadataRef;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::Else;
use databend_common_sql::plans::InsertMultiTable;
use databend_common_sql::plans::Into;
use databend_common_sql::plans::Plan;
use databend_common_storages_fuse::FuseTable;

use super::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::common::build_update_stream_reqs;
use crate::physical_plans::CastSchema;
use crate::physical_plans::ChunkAppendData;
use crate::physical_plans::ChunkCastSchema;
use crate::physical_plans::ChunkCommitInsert;
use crate::physical_plans::ChunkEvalScalar;
use crate::physical_plans::ChunkFillAndReorder;
use crate::physical_plans::ChunkFilter;
use crate::physical_plans::ChunkMerge;
use crate::physical_plans::ChunkSerializeCommitMeta;
use crate::physical_plans::Duplicate;
use crate::physical_plans::EvalScalar;
use crate::physical_plans::Exchange;
use crate::physical_plans::FillAndReorder;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::MultiInsertEvalScalar;
use crate::physical_plans::MultiInsertRoute;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::SerializableTable;
use crate::physical_plans::Shuffle;
use crate::physical_plans::ShuffleStrategy;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;
use crate::sql::executor::cast_expr_to_non_null_boolean;
use crate::storages::Table;
use crate::stream::DataBlockStream;
pub struct InsertMultiTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertMultiTable,
}

impl InsertMultiTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertMultiTable) -> Result<InterpreterPtr> {
        Ok(Arc::new(Self { ctx, plan }))
    }

    pub fn try_create_static(ctx: Arc<QueryContext>, plan: InsertMultiTable) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertMultiTableInterpreter {
    fn name(&self) -> &str {
        "InsertMultiTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let physical_plan = self.build_physical_plan(false).await?;
            let mut build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
            // Execute hook.
            if self
                .ctx
                .get_settings()
                .get_enable_compact_after_multi_table_insert()?
            {
                for (_, (db, tbl)) in &self.plan.target_tables {
                    let hook_operator = HookOperator::create(
                        self.ctx.clone(),
                        // multi table insert only support default catalog
                        CATALOG_DEFAULT.to_string(),
                        db.to_string(),
                        tbl.to_string(),
                        MutationKind::Insert,
                        LockTableOption::LockNoRetry,
                    );
                    hook_operator.execute(&mut build_res.main_pipeline).await;
                }
            }
            Ok(build_res)
        })
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let mut columns = vec![];
        let insert_rows = self.ctx.mutation_state().multi_table_insert_rows();
        let guard = insert_rows.lock().unwrap();
        for (tid, _) in &self.plan.target_tables {
            let insert_row = guard.get(tid).cloned().unwrap_or_default();
            columns.push(UInt64Type::from_data(vec![insert_row]));
        }
        let blocks = vec![DataBlock::new_from_columns(columns)];
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

impl InsertMultiTableInterpreter {
    pub async fn build_physical_plan(&self, dry_run: bool) -> Result<PhysicalPlan> {
        let (mut root, route_columns) = self.build_source_physical_plan().await?;
        let update_stream_meta = build_update_stream_reqs(&self.ctx, false).await?;
        let source_schema = root.output_schema()?;
        let branches = self.build_insert_into_branches().await?;
        let serializable_tables = branches
            .build_serializable_target_tables(self.ctx.clone(), dry_run)
            .await?;
        let deduplicated_serializable_tables = branches
            .build_deduplicated_serializable_target_tables(self.ctx.clone(), dry_run)
            .await?;
        let predicates = branches.build_predicates(source_schema.as_ref(), &route_columns)?;
        let eval_scalars = branches.build_eval_scalars(source_schema.as_ref())?;

        // Source schemas for each branch to be casted to the target schema
        // It may be output of eval scalar(if exists) or just the source subquery's schema
        let source_schemas = eval_scalars
            .iter()
            .map(|opt_exprs| {
                opt_exprs
                    .as_ref()
                    .map(
                        |MultiInsertEvalScalar {
                             remote_exprs,
                             projection: _,
                         }| {
                            let mut evaled_fields = Vec::with_capacity(remote_exprs.len());
                            for eval_scalar_expr in remote_exprs {
                                let data_type = eval_scalar_expr
                                    .as_expr(&BUILTIN_FUNCTIONS)
                                    .data_type()
                                    .clone();
                                evaled_fields.push(DataField::new("", data_type));
                            }
                            Arc::new(DataSchema::new(evaled_fields))
                        },
                    )
                    .unwrap_or_else(|| source_schema.clone())
            })
            .collect::<Vec<_>>();
        let cast_schemas = branches.build_cast_schema(source_schemas);
        let fill_and_reorders = branches.build_fill_and_reorder(self.ctx.clone()).await?;
        let group_ids = branches.build_group_ids();

        let top_exchange = Exchange::from_physical_plan(&root);
        if let Some(exchange) = top_exchange
            && exchange.kind != FragmentKind::Merge
        {
            return Err(ErrorCode::Internal(format!(
                "unexpected top-level exchange kind for multi-table insert: {:?}",
                exchange.kind
            )));
        }

        let distributed_insert = branches.support_distributed_insert() && top_exchange.is_some();
        let input = if let Some(exchange) = top_exchange.filter(|_| distributed_insert) {
            let input = exchange.input.clone();
            let remote_write_plan = Self::build_write_plan(
                input,
                branches.len(),
                predicates,
                eval_scalars,
                cast_schemas,
                fill_and_reorders,
                serializable_tables.clone(),
            );
            let remote_commit_input = PhysicalPlan::new(ChunkMerge {
                group_ids: group_ids.clone(),
                input: remote_write_plan,
                meta: PhysicalPlanMeta::new("ChunkMerge"),
            });
            let remote_commit_meta = PhysicalPlan::new(ChunkSerializeCommitMeta {
                input: remote_commit_input,
                targets: deduplicated_serializable_tables.clone(),
                meta: PhysicalPlanMeta::new("CommitMeta"),
            });
            exchange.derive(vec![remote_commit_meta])
        } else {
            let write_plan = Self::build_write_plan(
                root,
                branches.len(),
                predicates,
                eval_scalars,
                cast_schemas,
                fill_and_reorders,
                serializable_tables.clone(),
            );
            PhysicalPlan::new(ChunkMerge {
                group_ids,
                input: write_plan,
                meta: PhysicalPlanMeta::new("ChunkMerge"),
            })
        };

        root = PhysicalPlan::new(ChunkCommitInsert {
            update_stream_meta,

            input,
            overwrite: self.plan.overwrite,
            input_commit_meta: distributed_insert,
            targets: deduplicated_serializable_tables,
            meta: PhysicalPlanMeta::new("Commit"),
        });

        let mut next_plan_id = 0;
        root.adjust_plan_id(&mut next_plan_id);
        Ok(root)
    }

    fn build_write_plan(
        input: PhysicalPlan,
        branch_len: usize,
        predicates: Vec<Option<RemoteExpr>>,
        eval_scalars: Vec<Option<MultiInsertEvalScalar>>,
        cast_schemas: Vec<Option<CastSchema>>,
        fill_and_reorders: Vec<Option<FillAndReorder>>,
        target_tables: Vec<SerializableTable>,
    ) -> PhysicalPlan {
        let root = PhysicalPlan::new(Duplicate {
            input,
            n: branch_len,
            meta: PhysicalPlanMeta::new("Duplicate"),
        });

        let root = PhysicalPlan::new(Shuffle {
            input: root,
            strategy: ShuffleStrategy::Transpose(branch_len),
            meta: PhysicalPlanMeta::new("Shuffle"),
        });

        let root = PhysicalPlan::new(ChunkFilter {
            predicates,
            input: root,
            meta: PhysicalPlanMeta::new("ChunkFilter"),
        });

        let root = PhysicalPlan::new(ChunkEvalScalar {
            eval_scalars,
            input: root,
            meta: PhysicalPlanMeta::new("ChunkEvalScalar"),
        });

        let root = PhysicalPlan::new(ChunkCastSchema {
            cast_schemas,
            input: root,
            meta: PhysicalPlanMeta::new("ChunkCastSchema"),
        });

        let root = PhysicalPlan::new(ChunkFillAndReorder {
            fill_and_reorders,
            input: root,
            meta: PhysicalPlanMeta::new("ChunkFillAndReorder"),
        });

        PhysicalPlan::new(ChunkAppendData {
            input: root,
            target_tables,
            meta: PhysicalPlanMeta::new("ChunkAppendData"),
        })
    }

    /// Plans the source query and appends the routing decision of every row.
    ///
    /// The output starts with the source's logical result columns, in order, followed by the
    /// route columns. Branches without VALUES(...) insert the leading columns positionally.
    async fn build_source_physical_plan(&self) -> Result<(PhysicalPlan, RouteColumns)> {
        let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = &self.plan.input_source
        else {
            unreachable!()
        };

        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
        let mut required = bind_context.column_set();
        required.extend(self.source_required_columns());
        let input_source = builder.build_query(s_expr.planned()?, required).await?;

        // `build_physical_plan` distributes the write only when the source root is a Merge
        // exchange. Routing and projection are row-wise and the exchange output follows its
        // input, so put them below the exchange rather than wrapping (and hiding) it.
        match Exchange::from_physical_plan(&input_source) {
            Some(exchange) if exchange.kind == FragmentKind::Merge => {
                let (input, route_columns) =
                    self.route_and_project(exchange.input.clone(), bind_context, metadata)?;
                Ok((exchange.derive(vec![input]), route_columns))
            }
            _ => self.route_and_project(input_source, bind_context, metadata),
        }
    }

    fn route_and_project(
        &self,
        input: PhysicalPlan,
        bind_context: &BindContext,
        metadata: &MetadataRef,
    ) -> Result<(PhysicalPlan, RouteColumns)> {
        let (input, route_columns) = self.build_route(input, metadata)?;

        // Lazy materialization (triggered by WHERE + LIMIT) may reorder physical output columns
        // and inject internal columns like _row_id, and WHEN conditions may read columns outside
        // the logical output. Keep the logical output columns, in order, then the route columns.
        let schema = input.output_schema()?;
        let col_refs: Vec<(usize, Symbol)> = bind_context
            .result_columns()
            .iter()
            .map(|(sym, _)| *sym)
            .chain(route_columns.symbols())
            .map(|sym| {
                let pos = schema.index_of(&sym.to_string()).map_err(|_| {
                    ErrorCode::Internal(format!(
                        "Failed to map source column {sym} for multi-table insert"
                    ))
                })?;
                Ok((pos, sym))
            })
            .collect::<Result<_>>()?;

        // If the schema already has exactly these columns in this order, skip the projection.
        let needs_projection = schema.num_fields() != col_refs.len()
            || col_refs
                .iter()
                .enumerate()
                .any(|(expected, &(pos, _))| expected != pos);
        if !needs_projection {
            return Ok((input, route_columns));
        }

        let exprs: Vec<(RemoteExpr, Symbol)> = col_refs
            .iter()
            .map(|&(pos, sym)| {
                let remote_expr = RemoteExpr::ColumnRef {
                    span: None,
                    id: pos,
                    data_type: schema.field(pos).data_type().clone(),
                    display_name: sym.to_string(),
                };
                (remote_expr, sym)
            })
            .collect();

        // Project only the new expr columns (input_col_num .. input_col_num + n).
        let input_col_num = schema.num_fields();
        let projections: BTreeSet<usize> = (input_col_num..input_col_num + exprs.len()).collect();
        let projected = PhysicalPlan::new(EvalScalar::create(input, exprs, projections, None));
        Ok((projected, route_columns))
    }

    /// Evaluates every WHEN condition exactly once per source row, before the rows fan out to
    /// the branches, and appends one boolean column per WHEN. All branches then filter on the
    /// same decision, which keeps volatile conditions consistent across INTO clauses and lets a
    /// NULL condition mean "not matched" without affecting the WHEN clauses after it.
    fn build_route(
        &self,
        input: PhysicalPlan,
        metadata: &MetadataRef,
    ) -> Result<(PhysicalPlan, RouteColumns)> {
        if self.plan.whens.is_empty() {
            return Ok((input, RouteColumns::Unconditional));
        }
        let schema = input.output_schema()?;
        let mut conditions = Vec::with_capacity(self.plan.whens.len());
        let mut columns = Vec::with_capacity(self.plan.whens.len());
        for (idx, when) in self.plan.whens.iter().enumerate() {
            let expr = when
                .condition
                .as_expr()?
                .project_column_ref(|col| schema.index_of(&col.index.to_string()))?;
            conditions.push(cast_expr_to_non_null_boolean(expr)?.as_remote_expr());
            let name = format!("_insert_multi_matched_{idx}");
            let symbol = metadata
                .write()
                .add_derived_column(name.clone(), DataType::Boolean);
            columns.push(RouteColumn { symbol, name });
        }
        let route = PhysicalPlan::new(MultiInsertRoute {
            meta: PhysicalPlanMeta::new("MultiInsertRoute"),
            input,
            is_first: self.plan.is_first,
            conditions,
            route_symbols: columns.iter().map(|column| column.symbol).collect(),
        });
        Ok((route, RouteColumns::Matched(columns)))
    }

    fn source_required_columns(&self) -> ColumnSet {
        let mut required = ColumnSet::new();
        for when in &self.plan.whens {
            required.extend(when.condition.used_columns());
        }
        required
    }

    async fn build_insert_into_branches(&self) -> Result<InsertIntoBranches> {
        let InsertMultiTable {
            input_source: _,
            whens,
            opt_else,
            overwrite: _,
            is_first: _,
            intos,
            target_tables: _,
            meta_data: _,
        } = &self.plan;
        let mut branches = InsertIntoBranches::default();
        let mut route_intos = vec![];
        for (idx, when) in whens.iter().enumerate() {
            for into in &when.intos {
                route_intos.push((BranchRoute::When(idx), into));
            }
        }
        if let Some(Else { intos }) = opt_else {
            for into in intos {
                route_intos.push((BranchRoute::Else, into));
            }
        }
        for into in intos {
            route_intos.push((BranchRoute::Always, into));
        }

        route_intos.sort_by(|a, b| {
            a.1.catalog
                .cmp(&b.1.catalog)
                .then(a.1.database.cmp(&b.1.database))
                .then(a.1.table.cmp(&b.1.table))
        });

        for (route, into) in route_intos {
            let Into {
                catalog,
                database,
                table,
                casted_schema,
                source_scalar_exprs,
                ..
            } = into;
            let table_name = table;
            let table = self.ctx.get_table(catalog, database, table_name).await?;
            table.check_mutable()?;
            self.ctx.update_query_lineage_target_id(
                catalog,
                database,
                table_name,
                table.get_table_info().ident.table_id,
            );
            branches.push(
                table,
                route,
                source_scalar_exprs.clone(),
                casted_schema.clone(),
            );
        }

        Ok(branches)
    }
}

/// The rows an INTO branch receives.
#[derive(Clone, Copy)]
enum BranchRoute {
    /// An unconditional INTO: every source row.
    Always,
    /// An INTO of the WHEN clause at this position.
    When(usize),
    /// An INTO of the ELSE clause: the rows for which no WHEN condition is true.
    Else,
}

/// A column the source appends to carry the routing decision of each row.
struct RouteColumn {
    symbol: Symbol,
    name: String,
}

/// The routing decision of each source row, computed once before the rows fan out.
enum RouteColumns {
    /// There is no WHEN clause.
    Unconditional,
    /// One non-null boolean column per WHEN clause, true when the row goes to that clause.
    /// For INSERT FIRST at most one of them is true for a row.
    Matched(Vec<RouteColumn>),
}

impl RouteColumns {
    fn symbols(&self) -> impl Iterator<Item = Symbol> + '_ {
        let columns = match self {
            RouteColumns::Unconditional => &[][..],
            RouteColumns::Matched(columns) => columns.as_slice(),
        };
        columns.iter().map(|column| column.symbol)
    }

    fn branch_filter(&self, route: BranchRoute, schema: &DataSchema) -> Result<Option<RemoteExpr>> {
        let filter = match (self, route) {
            (_, BranchRoute::Always) => return Ok(None),
            (RouteColumns::Matched(columns), BranchRoute::When(idx)) => {
                route_column_ref(&columns[idx], schema)?
            }
            (RouteColumns::Matched(columns), BranchRoute::Else) => {
                let mut matched_any: Option<Expr> = None;
                for column in columns {
                    let matched = route_column_ref(column, schema)?;
                    matched_any = Some(match matched_any {
                        None => matched,
                        Some(prev) => {
                            check_function(None, "or", &[], &[prev, matched], &BUILTIN_FUNCTIONS)?
                        }
                    });
                }
                let matched_any = matched_any.ok_or_else(|| {
                    ErrorCode::Internal("multi-table insert ELSE requires a WHEN clause")
                })?;
                check_function(None, "not", &[], &[matched_any], &BUILTIN_FUNCTIONS)?
            }
            (RouteColumns::Unconditional, BranchRoute::When(_) | BranchRoute::Else) => {
                return Err(ErrorCode::Internal(
                    "multi-table insert branch is conditional but there is no WHEN clause",
                ));
            }
        };
        Ok(Some(filter.as_remote_expr()))
    }
}

fn route_column_ref(column: &RouteColumn, schema: &DataSchema) -> Result<Expr> {
    let id = schema.index_of(&column.symbol.to_string())?;
    Ok(Expr::ColumnRef(ColumnRef {
        span: None,
        id,
        data_type: schema.field(id).data_type().clone(),
        display_name: format!("{} (#{})", column.name, column.symbol),
    }))
}

pub(crate) fn scalar_expr_to_remote_expr(
    expr: &ScalarExpr,
    block_schema: &DataSchema,
) -> Result<RemoteExpr> {
    let expr = expr
        .as_expr()?
        .project_column_ref(|col| block_schema.index_of(&col.index.to_string()))?;
    Ok(expr.as_remote_expr())
}

#[derive(Default)]
struct InsertIntoBranches {
    tables: Vec<Arc<dyn Table>>,
    routes: Vec<BranchRoute>,
    source_exprs: Vec<Option<Vec<ScalarExpr>>>,
    casted_schemas: Vec<DataSchemaRef>,
    len: usize,
}

impl InsertIntoBranches {
    fn push(
        &mut self,
        table: Arc<dyn Table>,
        route: BranchRoute,
        source_exprs: Option<Vec<ScalarExpr>>,
        casted_schema: DataSchemaRef,
    ) {
        self.tables.push(table);
        self.routes.push(route);
        self.source_exprs.push(source_exprs);
        self.casted_schemas.push(casted_schema);
        self.len += 1;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn support_distributed_insert(&self) -> bool {
        self.tables
            .iter()
            .all(|table| table.support_distributed_insert() && !table.is_temp())
    }

    async fn build_serializable_target_tables(
        &self,
        ctx: Arc<QueryContext>,
        dry_run: bool,
    ) -> Result<Vec<SerializableTable>> {
        let mut serializable_tables = vec![];
        for table in &self.tables {
            let table_info = table.get_table_info();
            let catalog_info = ctx.get_catalog(table_info.catalog()).await?.info();
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let snapshot = fuse_table.read_table_snapshot().await?;
            let table_meta_timestamps = if dry_run {
                ctx.get_table_meta_timestamps_without_txn_record(table.as_ref(), snapshot)?
            } else {
                ctx.get_table_meta_timestamps(table.as_ref(), snapshot)?
            };
            serializable_tables.push(SerializableTable {
                target_catalog_info: catalog_info,
                target_table_info: table_info.clone(),
                table_meta_timestamps,
            });
        }
        Ok(serializable_tables)
    }

    async fn build_deduplicated_serializable_target_tables(
        &self,
        ctx: Arc<QueryContext>,
        dry_run: bool,
    ) -> Result<Vec<SerializableTable>> {
        let mut serializable_tables = vec![];
        let mut last_table_id = None;
        for table in &self.tables {
            let table_info = table.get_table_info();
            let table_id = table_info.ident.table_id;
            if last_table_id == Some(table_id) {
                continue;
            }
            last_table_id = Some(table_id);
            let catalog_info = ctx.get_catalog(table_info.catalog()).await?.info();
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let snapshot = fuse_table.read_table_snapshot().await?;
            let table_meta_timestamps = if dry_run {
                ctx.get_table_meta_timestamps_without_txn_record(table.as_ref(), snapshot)?
            } else {
                ctx.get_table_meta_timestamps(table.as_ref(), snapshot)?
            };
            serializable_tables.push(SerializableTable {
                target_catalog_info: catalog_info,
                target_table_info: table_info.clone(),
                table_meta_timestamps,
            });
        }
        Ok(serializable_tables)
    }

    fn build_predicates(
        &self,
        source_schema: &DataSchema,
        route_columns: &RouteColumns,
    ) -> Result<Vec<Option<RemoteExpr>>> {
        self.routes
            .iter()
            .map(|route| route_columns.branch_filter(*route, source_schema))
            .collect()
    }

    fn build_eval_scalars(
        &self,
        source_schema: &DataSchema,
    ) -> Result<Vec<Option<MultiInsertEvalScalar>>> {
        let mut eval_scalars = vec![];
        for opt_scalar_exprs in self.source_exprs.iter() {
            if let Some(scalar_exprs) = opt_scalar_exprs {
                let exprs = scalar_exprs
                    .iter()
                    .map(|scalar_expr| scalar_expr_to_remote_expr(scalar_expr, source_schema))
                    .collect::<Result<Vec<_>>>()?;
                let source_field_num = source_schema.fields().len();
                let evaled_num = scalar_exprs.len();
                let projection = (source_field_num..source_field_num + evaled_num).collect();
                eval_scalars.push(Some(MultiInsertEvalScalar {
                    remote_exprs: exprs,
                    projection,
                }));
            } else {
                eval_scalars.push(None);
            }
        }
        Ok(eval_scalars)
    }

    fn build_cast_schema(&self, source_schemas: Vec<DataSchemaRef>) -> Vec<Option<CastSchema>> {
        self.casted_schemas
            .iter()
            .zip(source_schemas.iter())
            .map(|(casted_schema, source_schema)| {
                if casted_schema != source_schema {
                    Some(CastSchema {
                        source_schema: source_schema.clone(),
                        target_schema: casted_schema.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    async fn build_fill_and_reorder(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Vec<Option<FillAndReorder>>> {
        let mut fill_and_reorders = vec![];
        for (table, casted_schema) in self.tables.iter().zip(self.casted_schemas.iter()) {
            let target_schema: DataSchemaRef = Arc::new(table.schema().into());
            if target_schema.as_ref() != casted_schema.as_ref() {
                let table_info = table.get_table_info();
                fill_and_reorders.push(Some(FillAndReorder {
                    source_schema: casted_schema.clone(),
                    target_table_info: table_info.clone(),
                }));
            } else {
                fill_and_reorders.push(None);
            }
        }
        Ok(fill_and_reorders)
    }

    fn build_group_ids(&self) -> Vec<u64> {
        self.tables.iter().map(|table| table.get_id()).collect()
    }
}
