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

use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ROW_ID_COLUMN_ID;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::DeleteSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::optimize_query;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::Filter;
use databend_common_sql::plans::ModifyBySubquery;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::RemoteSubqueryMutation;
use databend_common_sql::plans::SubqueryDesc;
use databend_common_sql::MetadataRef;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::TruncateMode;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::debug;

use crate::interpreters::common::create_push_down_filters;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::locks::LockManager;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::DeletePlan;

/// interprets DeletePlan
pub struct DeleteInterpreter {
    ctx: Arc<QueryContext>,
    plan: DeletePlan,
}

impl DeleteInterpreter {
    /// Create the DeleteInterpreter from DeletePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<Self> {
        Ok(DeleteInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DeleteInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "DeleteInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "delete_interpreter_execute");

        let is_distributed = !self.ctx.get_cluster().is_empty();

        let catalog_name = self.plan.catalog_name.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let catalog_info = catalog.info();

        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();
        let tbl = catalog
            .get_table(&self.ctx.get_tenant(), db_name, tbl_name)
            .await?;

        // Add table lock.
        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        let lock_guard = table_lock.try_lock(self.ctx.clone()).await?;

        // refresh table.
        let tbl = tbl.refresh(self.ctx.as_ref()).await?;

        // check mutability
        tbl.check_mutable()?;

        let fuse_table = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support DELETE FROM",
                tbl.name(),
                tbl.get_table_info().engine(),
            ))
        })?;

        if let Some(subquery_desc) = &self.plan.subquery_desc {
            let support_row_id = tbl.supported_internal_column(ROW_ID_COLUMN_ID);
            if !support_row_id {
                return Err(ErrorCode::from_string(format!(
                    "Delete with subquery is not supported for the table '{}', which lacks row_id support.",
                    tbl.name(),
                )));
            }

            let mut build_res = modify_by_subquery(
                tbl.clone(),
                subquery_desc.clone(),
                self.plan.metadata.clone(),
                self.ctx.clone(),
                RemoteSubqueryMutation::Delete,
                is_distributed,
                fuse_table.get_table_info().clone(),
                catalog_info.clone(),
            )
            .await?;
            build_res.main_pipeline.add_lock_guard(lock_guard);
            return Ok(build_res);
        }

        let selection = self.plan.selection.clone();

        let (filters, col_indices) = if let Some(scalar) = selection {
            // prepare the filter expression
            let filters = create_push_down_filters(&scalar)?;

            let expr = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Delete must have deterministic predicate",
                ));
            }

            let used_columns = scalar.used_columns().clone();
            let col_indices: Vec<usize> = used_columns.into_iter().collect();
            (Some(filters), col_indices)
        } else {
            (None, vec![])
        };

        let mut build_res = PipelineBuildResult::create();

        // check if table is empty
        let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
            // no snapshot, no deletion
            return Ok(build_res);
        };
        if snapshot.summary.row_count == 0 {
            // empty snapshot, no deletion
            return Ok(build_res);
        }

        build_res.main_pipeline.add_lock_guard(lock_guard);
        // check if unconditional deletion
        let Some(filters) = filters else {
            let progress_values = ProgressValues {
                rows: snapshot.summary.row_count as usize,
                bytes: snapshot.summary.uncompressed_byte_size as usize,
            };
            self.ctx.get_write_progress().incr(&progress_values);
            // deleting the whole table... just a truncate
            fuse_table
                .do_truncate(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    TruncateMode::Delete,
                )
                .await?;
            return Ok(build_res);
        };

        if col_indices.is_empty() {
            // here the situation: filter_expr is not null, but col_indices in empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            if fuse_table.try_eval_const(self.ctx.clone(), &fuse_table.schema(), &filters.filter)? {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                self.ctx.get_write_progress().incr(&progress_values);

                // deleting the whole table... just a truncate
                fuse_table
                    .do_truncate(
                        self.ctx.clone(),
                        &mut build_res.main_pipeline,
                        TruncateMode::Delete,
                    )
                    .await?;
                return Ok(build_res);
            }
        }

        let cluster = self.ctx.get_cluster();
        let is_lazy = !cluster.is_empty() && snapshot.segments.len() >= cluster.nodes.len();
        let partitions = fuse_table
            .mutation_read_partitions(
                self.ctx.clone(),
                snapshot.clone(),
                col_indices.clone(),
                Some(filters.clone()),
                is_lazy,
                true,
            )
            .await?;

        let physical_plan = Self::build_physical_plan(
            filters,
            partitions,
            fuse_table.get_table_info().clone(),
            col_indices,
            snapshot,
            catalog_info,
            is_distributed,
        )?;

        build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                catalog_name.to_string(),
                db_name.to_string(),
                tbl_name.to_string(),
                MutationKind::Delete,
                // table lock has been added, no need to check.
                false,
            );
            hook_operator
                .execute_refresh(&mut build_res.main_pipeline)
                .await;
        }

        Ok(build_res)
    }
}

impl DeleteInterpreter {
    #[allow(clippy::too_many_arguments)]
    pub fn build_physical_plan(
        filters: Filters,
        partitions: Partitions,
        table_info: TableInfo,
        col_indices: Vec<usize>,
        snapshot: Arc<TableSnapshot>,
        catalog_info: CatalogInfo,
        is_distributed: bool,
    ) -> Result<PhysicalPlan> {
        let merge_meta = partitions.partitions_type() == PartInfoType::LazyLevel;
        let mut root = PhysicalPlan::DeleteSource(Box::new(DeleteSource {
            parts: partitions,
            filters,
            table_info: table_info.clone(),
            catalog_info: catalog_info.clone(),
            col_indices,
            query_row_id_col: false,
            snapshot: snapshot.clone(),
            plan_id: u32::MAX,
        }));

        if is_distributed {
            root = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            });
        }
        let mut plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            snapshot,
            table_info,
            catalog_info,
            mutation_kind: MutationKind::Delete,
            update_stream_meta: vec![],
            merge_meta,
            need_lock: false,
            deduplicated_label: None,
            plan_id: u32::MAX,
        }));
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}

pub async fn modify_by_subquery(
    table: Arc<dyn Table>,
    subquery_desc: SubqueryDesc,
    metadata: MetadataRef,
    ctx: Arc<QueryContext>,
    typ: RemoteSubqueryMutation,
    is_distributed: bool,
    table_info: TableInfo,
    catalog_info: CatalogInfo,
) -> Result<PipelineBuildResult> {
    let mut subquery_desc = subquery_desc;
    subquery_desc
        .outer_columns
        .extend(subquery_desc.predicate_columns.iter());

    // 1: optimize subquery expression
    let input_expr = &subquery_desc.input_expr;
    let outer_columns = &subquery_desc.outer_columns;
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone())
        .with_enable_distributed_optimization(is_distributed)
        .with_enable_join_reorder(unsafe { !ctx.get_settings().get_disable_join_reorder()? })
        .with_enable_dphyp(ctx.get_settings().get_enable_dphyp()?);
    let input_expr = optimize_query(opt_ctx, input_expr.clone()).await?;

    // 2: build filter executor with optimized subquery expression
    let filter = build_filter_plan(
        subquery_desc.clone(),
        metadata.clone(),
        input_expr.clone(),
        ctx.clone(),
    )
    .await?;

    let table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = if let Some(snapshot) = table.read_table_snapshot().await? {
        snapshot
    } else {
        return Err(ErrorCode::from_string(format!(
            "read table {:?} snapshot failed",
            table.name()
        )));
    };

    let modify_by_subquery = ModifyBySubquery {
        filter,
        snapshot,
        output_columns: outer_columns.clone(),
        subquery_desc: subquery_desc.clone(),
        typ,
        table_info,
        catalog_info,
    }
    .into();

    let root = SExpr::create_binary(
        Arc::new(modify_by_subquery),
        Arc::new(input_expr.clone()),
        Arc::new(input_expr.child(0)?.clone()),
    );

    // 3: build pipelines
    let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), false);
    let mut root = builder.build(&root, outer_columns.clone()).await?;

    // distribute the root source
    if is_distributed {
        root = PhysicalPlan::Exchange(Exchange {
            plan_id: 0,
            input: Box::new(root),
            kind: FragmentKind::Merge,
            keys: vec![],
            allow_adjust_parallelism: true,
            ignore_exchange: false,
        });
    }

    let build_res = build_query_pipeline_without_render_result_set(&ctx, &root).await?;

    Ok(build_res)
}

async fn build_filter_plan(
    subquery: SubqueryDesc,
    metadata: MetadataRef,
    subquery_expression: SExpr,
    ctx: Arc<QueryContext>,
) -> Result<Box<PhysicalPlan>> {
    let predicate = &subquery.predicate;
    let outer_columns = subquery.outer_columns.clone();

    let filter_expr = match subquery_expression.plan() {
        RelOperator::Filter(_) => subquery_expression,
        // in SQL like `update t1 set a = a + 1 where 200 > (select avg(a) from t1);`,
        // which subquery datatype is not boolean, in this case MUST filter the result as boolean.
        _ => {
            let filter = Filter {
                predicates: vec![predicate.clone()],
            };
            SExpr::create_unary(Arc::new(filter.into()), Arc::new(subquery_expression))
        }
    };

    let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), false);
    let filter = builder.build(&filter_expr, outer_columns).await?;

    Ok(Box::new(filter))
}
