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
use std::time::Instant;
use std::u64::MAX;

use common_catalog::lock_api::LockApi;
use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::SendableDataBlockStream;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableInfo;
use common_sql::executor::CommitSink;
use common_sql::executor::MergeInto;
use common_sql::executor::MergeIntoSource;
use common_sql::executor::MutationKind;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::optimizer::SExpr;
use common_sql::plans::Aggregate;
use common_sql::plans::AggregateFunction;
use common_sql::plans::AggregateMode;
use common_sql::plans::BoundColumnRef;
use common_sql::plans::ConstantExpr;
use common_sql::plans::EvalScalar;
use common_sql::plans::FunctionCall;
use common_sql::plans::MergeInto as MergePlan;
use common_sql::plans::Plan;
use common_sql::plans::RelOperator;
use common_sql::plans::ScalarItem;
use common_sql::plans::UpdatePlan;
use common_sql::BindContext;
use common_sql::ColumnBindingBuilder;
use common_sql::IndexType;
use common_sql::MetadataRef;
use common_sql::ScalarExpr;
use common_sql::TypeCheck;
use common_sql::Visibility;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use common_storages_fuse::TableContext;
use itertools::Itertools;
use storages_common_locks::LockManager;
use storages_common_table_meta::meta::TableSnapshot;
use tokio_stream::StreamExt;

use super::Interpreter;
use super::InterpreterPtr;
use crate::interpreters::common::hook_compact;
use crate::interpreters::common::CompactHookTraceCtx;
use crate::interpreters::common::CompactTargetTableDescription;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;

// predicate_index should not be conflict with update expr's column_binding's index.
pub const PREDICATE_COLUMN_INDEX: IndexType = MAX as usize;
const DUMMY_COL_INDEX: usize = 1;
pub struct MergeIntoInterpreter {
    ctx: Arc<QueryContext>,
    plan: MergePlan,
}

impl MergeIntoInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: MergePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(MergeIntoInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for MergeIntoInterpreter {
    fn name(&self) -> &str {
        "MergeIntoInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let start = Instant::now();
        let (physical_plan, table_info) = self.build_physical_plan().await?;
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan, false)
                .await?;

        // Add table lock before execution.
        let table_lock = LockManager::create_table_lock(self.ctx.clone(), table_info);
        let lock_guard = table_lock.try_lock(self.ctx.clone()).await?;
        build_res.main_pipeline.add_lock_guard(lock_guard);

        // Compact if 'enable_recluster_after_write' on.
        {
            let compact_target = CompactTargetTableDescription {
                catalog: self.plan.catalog.clone(),
                database: self.plan.database.clone(),
                table: self.plan.table.clone(),
            };

            let compact_hook_trace_ctx = CompactHookTraceCtx {
                start,
                operation_name: "merge_into".to_owned(),
            };

            hook_compact(
                self.ctx.clone(),
                &mut build_res.main_pipeline,
                compact_target,
                compact_hook_trace_ctx,
                false,
            )
            .await;
        }

        Ok(build_res)
    }
}

impl MergeIntoInterpreter {
    async fn build_physical_plan(&self) -> Result<(PhysicalPlan, TableInfo)> {
        let MergePlan {
            bind_context,
            input,
            meta_data,
            columns_set,
            catalog,
            database,
            table: table_name,
            target_alias,
            matched_evaluators,
            unmatched_evaluators,
            target_table_idx,
            field_index_map,
            ..
        } = &self.plan;

        // check mutability
        let table = self.ctx.get_table(catalog, database, table_name).await?;
        table.check_mutable()?;

        let optimized_input = self
            .build_static_filter(input, meta_data, self.ctx.clone())
            .await?;
        let mut builder = PhysicalPlanBuilder::new(meta_data.clone(), self.ctx.clone(), false);

        // build source for MergeInto
        let join_input = builder
            .build(&optimized_input, *columns_set.clone())
            .await?;

        // find row_id column index
        let join_output_schema = join_input.output_schema()?;

        let mut row_id_idx = match meta_data
            .read()
            .row_id_index_by_table_index(*target_table_idx)
        {
            None => {
                return Err(ErrorCode::InvalidRowIdIndex(
                    "can't get internal row_id_idx when running merge into",
                ));
            }
            Some(row_id_idx) => row_id_idx,
        };

        let mut found_row_id = false;
        for (idx, data_field) in join_output_schema.fields().iter().enumerate() {
            if *data_field.name() == row_id_idx.to_string() {
                row_id_idx = idx;
                found_row_id = true;
                break;
            }
        }

        // we can't get row_id_idx, throw an exception
        if !found_row_id {
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_id_idx when running merge into",
            ));
        }

        let fuse_table =
            table
                .as_any()
                .downcast_ref::<FuseTable>()
                .ok_or(ErrorCode::Unimplemented(format!(
                    "table {}, engine type {}, does not support MERGE INTO",
                    table.name(),
                    table.get_table_info().engine(),
                )))?;

        let table_info = fuse_table.get_table_info().clone();
        let catalog_ = self.ctx.get_catalog(catalog).await?;

        // merge_into_source is used to recv join's datablocks and split them into macthed and not matched
        // datablocks.
        let merge_into_source = PhysicalPlan::MergeIntoSource(MergeIntoSource {
            input: Box::new(join_input),
            row_id_idx: row_id_idx as u32,
        });

        // transform unmatched for insert
        // reference to func `build_eval_scalar`
        // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
        let mut unmatched =
            Vec::<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>::with_capacity(
                unmatched_evaluators.len(),
            );

        for item in unmatched_evaluators {
            let filter = if let Some(filter_expr) = &item.condition {
                Some(self.transform_scalar_expr2expr(filter_expr, join_output_schema.clone())?)
            } else {
                None
            };

            let mut values_exprs = Vec::<RemoteExpr>::with_capacity(item.values.len());

            for scalar_expr in &item.values {
                values_exprs
                    .push(self.transform_scalar_expr2expr(scalar_expr, join_output_schema.clone())?)
            }

            unmatched.push((item.source_schema.clone(), filter, values_exprs))
        }

        // the first option is used for condition
        // the second option is used to distinct update and delete
        let mut matched = Vec::with_capacity(matched_evaluators.len());

        // transform matched for delete/update
        for item in matched_evaluators {
            let condition = if let Some(condition) = &item.condition {
                let expr = self
                    .transform_scalar_expr2expr(condition, join_output_schema.clone())?
                    .as_expr(&BUILTIN_FUNCTIONS);
                let (expr, _) = ConstantFolder::fold(
                    &expr,
                    &self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );
                Some(expr.as_remote_expr())
            } else {
                None
            };

            // update
            let update_list = if let Some(update_list) = &item.update {
                // use update_plan to get exprs
                let update_plan = UpdatePlan {
                    selection: None,
                    subquery_desc: vec![],
                    database: database.clone(),
                    table: match target_alias {
                        None => table_name.clone(),
                        Some(alias) => alias.name.to_string(),
                    },
                    update_list: update_list.clone(),
                    bind_context: bind_context.clone(),
                    metadata: self.plan.meta_data.clone(),
                    catalog: catalog.clone(),
                };
                // we don't need real col_indices here, just give a
                // dummy index, that's ok.
                let col_indices = vec![DUMMY_COL_INDEX];
                let update_list: Vec<(FieldIndex, RemoteExpr<String>)> = update_plan
                    .generate_update_list(
                        self.ctx.clone(),
                        fuse_table.schema().into(),
                        col_indices,
                        Some(PREDICATE_COLUMN_INDEX),
                        target_alias.is_some(),
                    )?;
                let update_list = update_list
                    .iter()
                    .map(|(idx, remote_expr)| {
                        (
                            *idx,
                            remote_expr
                                .as_expr(&BUILTIN_FUNCTIONS)
                                .project_column_ref(|name| {
                                    // there will add a predicate col when we process matched clauses.
                                    // so it's not in join_output_schema for now. But it's must be added
                                    // to the tail, so let do it like below.
                                    if *name == PREDICATE_COLUMN_INDEX.to_string() {
                                        join_output_schema.num_fields()
                                    } else {
                                        join_output_schema.index_of(name).unwrap()
                                    }
                                })
                                .as_remote_expr(),
                        )
                    })
                    .collect_vec();
                Some(update_list)
            } else {
                // delete
                None
            };
            matched.push((condition, update_list))
        }

        let base_snapshot = fuse_table.read_table_snapshot().await?.unwrap_or_else(|| {
            Arc::new(TableSnapshot::new_empty_snapshot(
                fuse_table.schema().as_ref().clone(),
            ))
        });

        let mut field_index_of_input_schema = HashMap::<FieldIndex, usize>::new();
        for (field_index, value) in field_index_map {
            field_index_of_input_schema
                .insert(*field_index, join_output_schema.index_of(value).unwrap());
        }

        // recv datablocks from matched upstream and unmatched upstream
        // transform and append dat
        let merge_into = PhysicalPlan::MergeInto(Box::new(MergeInto {
            input: Box::new(merge_into_source),
            table_info: table_info.clone(),
            catalog_info: catalog_.info(),
            unmatched,
            matched,
            field_index_of_input_schema,
            row_id_idx,
            segments: base_snapshot
                .segments
                .clone()
                .into_iter()
                .enumerate()
                .collect(),
        }));

        // build mutation_aggregate
        let physical_plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(merge_into),
            snapshot: base_snapshot,
            table_info: table_info.clone(),
            catalog_info: catalog_.info(),
            // let's use update first, we will do some optimizeations and select exact strategy
            mutation_kind: MutationKind::Update,
            merge_meta: false,
            need_lock: false,
        }));

        Ok((physical_plan, table_info))
    }

    async fn build_static_filter(
        &self,
        join: &SExpr,
        metadata: &MetadataRef,
        ctx: Arc<QueryContext>,
    ) -> Result<Box<SExpr>> {
        // 1. collect statistics from the source side
        // plan of source table is extended to:
        //
        // AggregateFinal(min(source_join_side_expr),max(source_join_side_expr))
        //        \
        //     AggregatePartial(min(source_join_side_expr),max(source_join_side_expr))
        //         \
        //       EvalScalar(source_join_side_expr)
        //          \
        //         SourcePlan
        let source_plan = join.child(0)?;
        let join_op = match join.plan() {
            RelOperator::Join(j) => j,
            _ => unreachable!(),
        };
        if join_op.left_conditions.len() != 1 || join_op.right_conditions.len() != 1 {
            return Ok(Box::new(join.clone()));
        }
        let source_side_expr = &join_op.left_conditions[0];
        let target_side_expr = &join_op.right_conditions[0];

        // eval source side join expr
        let source_side_join_expr_index = metadata.write().add_derived_column(
            "source_side_join_expr".to_string(),
            source_side_expr.data_type()?,
        );
        let source_side_join_expr_binding = ColumnBindingBuilder::new(
            "source_side_join_expr".to_string(),
            source_side_join_expr_index,
            Box::new(source_side_expr.data_type()?),
            Visibility::Visible,
        )
        .build();
        let evaled_source_side_join_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: source_side_join_expr_binding.clone(),
        });
        let eval_source_side_join_expr_op = EvalScalar {
            items: vec![ScalarItem {
                scalar: source_side_expr.clone(),
                index: source_side_join_expr_index,
            }],
        };
        let eval_target_side_condition_sexpr = SExpr::create_unary(
            Arc::new(eval_source_side_join_expr_op.into()),
            Arc::new(source_plan.clone()),
        );

        // eval min/max of source side join expr
        let min_display_name = format!("min({:?})", source_side_expr);
        let max_display_name = format!("max({:?})", source_side_expr);
        let min_index = metadata
            .write()
            .add_derived_column(min_display_name.clone(), source_side_expr.data_type()?);
        let max_index = metadata
            .write()
            .add_derived_column(max_display_name.clone(), source_side_expr.data_type()?);
        let mut bind_context = Box::new(BindContext::new());
        let min_binding = ColumnBindingBuilder::new(
            min_display_name.clone(),
            min_index,
            Box::new(source_side_expr.data_type()?),
            Visibility::Visible,
        )
        .build();
        let max_binding = ColumnBindingBuilder::new(
            max_display_name.clone(),
            max_index,
            Box::new(source_side_expr.data_type()?),
            Visibility::Visible,
        )
        .build();
        bind_context.columns = vec![min_binding.clone(), max_binding.clone()];
        let min = ScalarItem {
            scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                func_name: "min".to_string(),
                distinct: false,
                params: vec![],
                args: vec![evaled_source_side_join_expr.clone()],
                return_type: Box::new(source_side_expr.data_type()?),
                display_name: min_display_name.clone(),
            }),
            index: min_index,
        };
        let max = ScalarItem {
            scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                func_name: "max".to_string(),
                distinct: false,
                params: vec![],
                args: vec![evaled_source_side_join_expr],
                return_type: Box::new(source_side_expr.data_type()?),
                display_name: max_display_name.clone(),
            }),
            index: max_index,
        };
        let agg_partial_op = Aggregate {
            mode: AggregateMode::Partial,
            group_items: vec![],
            aggregate_functions: vec![min.clone(), max.clone()],
            from_distinct: false,
            limit: None,
            grouping_sets: None,
        };
        let agg_partial_sexpr = SExpr::create_unary(
            Arc::new(agg_partial_op.into()),
            Arc::new(eval_target_side_condition_sexpr),
        );
        let agg_final_op = Aggregate {
            mode: AggregateMode::Final,
            group_items: vec![],
            aggregate_functions: vec![min.clone(), max.clone()],
            from_distinct: false,
            limit: None,
            grouping_sets: None,
        };
        let agg_final_sexpr =
            SExpr::create_unary(Arc::new(agg_final_op.into()), Arc::new(agg_partial_sexpr));
        let plan = Plan::Query {
            s_expr: Box::new(agg_final_sexpr),
            metadata: metadata.clone(),
            bind_context,
            rewrite_kind: None,
            formatted_ast: None,
            ignore_result: false,
        };
        let interpreter: InterpreterPtr = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream: SendableDataBlockStream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.collect::<Result<Vec<_>>>().await?;

        debug_assert_eq!(blocks.len(), 1);
        let block = &blocks[0];
        debug_assert_eq!(block.num_columns(), 2);

        let get_scalar_expr = |block: &DataBlock, index: usize| {
            let block_entry = &block.columns()[index];
            let scalar = match &block_entry.value {
                common_expression::Value::Scalar(scalar) => scalar.clone(),
                common_expression::Value::Column(column) => {
                    debug_assert_eq!(column.len(), 1);
                    let value_ref = column.index(0).unwrap();
                    value_ref.to_owned()
                }
            };
            ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar,
            })
        };

        let min_scalar = get_scalar_expr(block, 0);
        let max_scalar = get_scalar_expr(block, 1);

        // 2. build filter and push down to target side
        let gte_min = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "gte".to_string(),
            params: vec![],
            arguments: vec![target_side_expr.clone(), min_scalar],
        });
        let lte_max = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "lte".to_string(),
            params: vec![],
            arguments: vec![target_side_expr.clone(), max_scalar],
        });

        let filters = vec![gte_min, lte_max];
        let mut target_plan = join.child(1)?.clone();
        Self::push_down_filters(&mut target_plan, &filters)?;
        let new_sexpr =
            join.replace_children(vec![Arc::new(source_plan.clone()), Arc::new(target_plan)]);
        Ok(Box::new(new_sexpr))
    }

    fn push_down_filters(s_expr: &mut SExpr, filters: &[ScalarExpr]) -> Result<()> {
        match s_expr.plan() {
            RelOperator::Scan(s) => {
                let mut new_scan = s.clone();
                if let Some(preds) = new_scan.push_down_predicates {
                    new_scan.push_down_predicates =
                        Some(preds.iter().chain(filters).cloned().collect());
                } else {
                    new_scan.push_down_predicates = Some(filters.to_vec());
                }
                *s_expr = SExpr::create_leaf(Arc::new(RelOperator::Scan(new_scan)));
            }
            RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_) => {
                let mut new_child = s_expr.child(0)?.clone();
                Self::push_down_filters(&mut new_child, filters)?;
                *s_expr = s_expr.replace_children(vec![Arc::new(new_child)]);
            }
            RelOperator::CteScan(_) => {}
            RelOperator::Join(_) => {}
            RelOperator::Exchange(_) => {}
            RelOperator::UnionAll(_) => {}
            RelOperator::DummyTableScan(_) => {}
            RelOperator::RuntimeFilterSource(_) => {}
            RelOperator::Window(_) => {}
            RelOperator::ProjectSet(_) => {}
            RelOperator::MaterializedCte(_) => {}
            RelOperator::Lambda(_) => {}
            RelOperator::ConstantTableScan(_) => {}
            RelOperator::Pattern(_) => {}
        }
        Ok(())
    }

    fn transform_scalar_expr2expr(
        &self,
        scalar_expr: &ScalarExpr,
        schema: DataSchemaRef,
    ) -> Result<RemoteExpr> {
        let scalar_expr = scalar_expr
            .type_check(schema.as_ref())?
            .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap());
        let (filer, _) = ConstantFolder::fold(
            &scalar_expr,
            &self.ctx.get_function_context().unwrap(),
            &BUILTIN_FUNCTIONS,
        );
        Ok(filer.as_remote_expr())
    }
}
