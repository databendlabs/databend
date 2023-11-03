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

use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_catalog::lock::Lock;
use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::SendableDataBlockStream;
use common_expression::ROW_NUMBER_COL_NAME;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableInfo;
use common_sql::bind_one_table;
use common_sql::executor::CommitSink;
use common_sql::executor::Exchange;
use common_sql::executor::FragmentKind::Merge;
use common_sql::executor::MergeInto;
use common_sql::executor::MergeIntoAppendNotMatched;
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
use common_sql::plans::JoinType;
use common_sql::plans::MergeInto as MergePlan;
use common_sql::plans::Plan;
use common_sql::plans::RelOperator;
use common_sql::plans::ScalarItem;
use common_sql::plans::UpdatePlan;
use common_sql::BindContext;
use common_sql::ColumnBinding;
use common_sql::ColumnBindingBuilder;
use common_sql::IndexType;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use common_sql::ScalarExpr;
use common_sql::TypeCheck;
use common_sql::TypeChecker;
use common_sql::Visibility;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use common_storages_fuse::TableContext;
use itertools::Itertools;
use log::info;
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

struct MergeStyleJoin<'a> {
    source_conditions: &'a [ScalarExpr],
    target_conditions: &'a [ScalarExpr],
    source_sexpr: &'a SExpr,
    target_sexpr: &'a SExpr,
}

impl MergeStyleJoin<'_> {
    pub fn new(join: &SExpr) -> MergeStyleJoin {
        let join_op = match join.plan() {
            RelOperator::Join(j) => j,
            _ => unreachable!(),
        };
        assert!(matches!(join_op.join_type, JoinType::Right));
        let source_conditions = &join_op.right_conditions;
        let target_conditions = &join_op.left_conditions;
        let source_sexpr = join.child(1).unwrap();
        let target_sexpr = join.child(0).unwrap();
        MergeStyleJoin {
            source_conditions,
            target_conditions,
            source_sexpr,
            target_sexpr,
        }
    }

    pub fn collect_column_map(&self) -> HashMap<String, ColumnBinding> {
        let mut column_map = HashMap::new();
        for (t, s) in self
            .target_conditions
            .iter()
            .zip(self.source_conditions.iter())
        {
            if let (ScalarExpr::BoundColumnRef(t_col), ScalarExpr::BoundColumnRef(s_col)) = (t, s) {
                column_map.insert(t_col.column.column_name.clone(), s_col.column.clone());
            }
        }
        column_map
    }
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
        let table_lock = LockManager::create_table_lock(table_info)?;
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
        let check_table = self.ctx.get_table(catalog, database, table_name).await?;
        check_table.check_mutable()?;

        let table_name = table_name.clone();
        let input = input.clone();
        let (exchange, input) = if let RelOperator::Exchange(exchange) = input.plan() {
            (Some(exchange), Box::new(input.child(0)?.clone()))
        } else {
            (None, input)
        };

        let optimized_input = self
            .build_static_filter(&input, meta_data, self.ctx.clone(), check_table)
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
        let mut row_number_idx = None;
        for (idx, data_field) in join_output_schema.fields().iter().enumerate() {
            if *data_field.name() == row_id_idx.to_string() {
                row_id_idx = idx;
                found_row_id = true;
                break;
            }
        }

        if exchange.is_some() {
            row_number_idx = Some(join_output_schema.index_of(ROW_NUMBER_COL_NAME)?);
        }

        // we can't get row_id_idx, throw an exception
        if !found_row_id {
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_id_idx when running merge into",
            ));
        }

        if exchange.is_some() && row_number_idx.is_none() {
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_number_idx when running merge into",
            ));
        }

        let table = self.ctx.get_table(catalog, database, &table_name).await?;
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

        let segments: Vec<_> = base_snapshot
            .segments
            .clone()
            .into_iter()
            .enumerate()
            .collect();

        let commit_input = if exchange.is_none() {
            // recv datablocks from matched upstream and unmatched upstream
            // transform and append dat
            PhysicalPlan::MergeInto(Box::new(MergeInto {
                input: Box::new(merge_into_source),
                table_info: table_info.clone(),
                catalog_info: catalog_.info(),
                unmatched,
                matched,
                field_index_of_input_schema,
                row_id_idx,
                segments,
                distributed: false,
                output_schema: DataSchemaRef::default(),
            }))
        } else {
            let merge_append = PhysicalPlan::MergeInto(Box::new(MergeInto {
                input: Box::new(merge_into_source.clone()),
                table_info: table_info.clone(),
                catalog_info: catalog_.info(),
                unmatched: unmatched.clone(),
                matched,
                field_index_of_input_schema,
                row_id_idx,
                segments,
                distributed: true,
                output_schema: DataSchemaRef::new(DataSchema::new(vec![
                    join_output_schema.fields[row_number_idx.unwrap()].clone(),
                ])),
            }));

            PhysicalPlan::MergeIntoAppendNotMatched(Box::new(MergeIntoAppendNotMatched {
                input: Box::new(PhysicalPlan::Exchange(Exchange {
                    plan_id: 0,
                    input: Box::new(merge_append),
                    kind: Merge,
                    keys: vec![],
                    ignore_exchange: false,
                })),
                table_info: table_info.clone(),
                catalog_info: catalog_.info(),
                unmatched: unmatched.clone(),
                input_schema: merge_into_source.output_schema()?,
            }))
        };

        // build mutation_aggregate
        let physical_plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(commit_input),
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
        table: Arc<dyn Table>,
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
        let m_join = MergeStyleJoin::new(join);
        let mut eval_scalar_items = Vec::with_capacity(m_join.source_conditions.len());
        let mut min_max_binding = Vec::with_capacity(m_join.source_conditions.len() * 2);
        let mut min_max_scalar_items = Vec::with_capacity(m_join.source_conditions.len() * 2);
        let mut group_items = vec![];
        if m_join.source_conditions.is_empty() {
            return Ok(Box::new(join.clone()));
        }
        let column_map = m_join.collect_column_map();
        let fuse_table =
            table
                .as_any()
                .downcast_ref::<FuseTable>()
                .ok_or(ErrorCode::Unimplemented(format!(
                    "table {}, engine type {}, does not support MERGE INTO",
                    table.name(),
                    table.get_table_info().engine(),
                )))?;
        let mut group_exprs = vec![];
        if let Some(cluster_key_str) = fuse_table.cluster_key_str() {
            let sql_dialect = Dialect::MySQL;
            let tokens = tokenize_sql(cluster_key_str)?;
            let ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect)?;
            let (mut bind_context, metadata) = bind_one_table(table)?;
            if !ast_exprs.is_empty() {
                let ast_expr = &ast_exprs[0];
                let name_resolution_ctx =
                    NameResolutionContext::try_from(ctx.get_settings().as_ref())?;
                let mut type_checker = TypeChecker::new(
                    &mut bind_context,
                    ctx.clone(),
                    &name_resolution_ctx,
                    metadata.clone(),
                    &[],
                    false,
                    false,
                );
                let (scalar_expr, _) = *type_checker.resolve(ast_expr).await?;
                let projected = scalar_expr.try_project_column_binding(|binding| {
                    column_map.get(&binding.column_name).cloned()
                });
                if let Some(p) = projected {
                    group_exprs.push(p);
                }
            }
        }
        for group_expr in group_exprs {
            let index = metadata
                .write()
                .add_derived_column("".to_string(), group_expr.data_type()?);
            let evaled = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "".to_string(),
                    index,
                    Box::new(group_expr.data_type()?),
                    Visibility::Visible,
                )
                .build(),
            });
            eval_scalar_items.push(ScalarItem {
                scalar: group_expr.clone(),
                index,
            });
            group_items.push(ScalarItem {
                scalar: evaled.clone(),
                index,
            });
        }
        for source_side_expr in m_join.source_conditions {
            // eval source side join expr
            let index = metadata
                .write()
                .add_derived_column("".to_string(), source_side_expr.data_type()?);
            let evaled = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "".to_string(),
                    index,
                    Box::new(source_side_expr.data_type()?),
                    Visibility::Visible,
                )
                .build(),
            });
            eval_scalar_items.push(ScalarItem {
                scalar: source_side_expr.clone(),
                index,
            });

            // eval min/max of source side join expr
            let min_display_name = format!("min({:?})", source_side_expr);
            let max_display_name = format!("max({:?})", source_side_expr);
            let min_index = metadata
                .write()
                .add_derived_column(min_display_name.clone(), source_side_expr.data_type()?);
            let max_index = metadata
                .write()
                .add_derived_column(max_display_name.clone(), source_side_expr.data_type()?);
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
            min_max_binding.push(min_binding);
            min_max_binding.push(max_binding);
            let min = ScalarItem {
                scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                    func_name: "min".to_string(),
                    distinct: false,
                    params: vec![],
                    args: vec![evaled.clone()],
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
                    args: vec![evaled],
                    return_type: Box::new(source_side_expr.data_type()?),
                    display_name: max_display_name.clone(),
                }),
                index: max_index,
            };
            min_max_scalar_items.push(min);
            min_max_scalar_items.push(max);
        }

        let eval_source_side_join_expr_op = EvalScalar {
            items: eval_scalar_items,
        };
        let source_plan = m_join.source_sexpr;
        let eval_target_side_condition_sexpr = if let RelOperator::Exchange(_) = source_plan.plan()
        {
            // there is another row_number operator here
            SExpr::create_unary(
                Arc::new(eval_source_side_join_expr_op.into()),
                Arc::new(source_plan.child(0)?.child(0)?.clone()),
            )
        } else {
            SExpr::create_unary(
                Arc::new(eval_source_side_join_expr_op.into()),
                Arc::new(source_plan.clone()),
            )
        };

        let mut bind_context = Box::new(BindContext::new());
        bind_context.columns = min_max_binding;

        let agg_partial_op = Aggregate {
            mode: AggregateMode::Partial,
            group_items: group_items.clone(),
            aggregate_functions: min_max_scalar_items.clone(),
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
            group_items,
            aggregate_functions: min_max_scalar_items,
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

        // 2. build filter and push down to target side
        let mut filters = Vec::with_capacity(m_join.target_conditions.len());

        for (i, target_side_expr) in m_join.target_conditions.iter().enumerate() {
            let mut filter_parts = vec![];
            for block in blocks.iter() {
                let block = block.convert_to_full();
                let min_column = block.get_by_offset(i * 2).value.as_column().unwrap();
                let max_column = block.get_by_offset(i * 2 + 1).value.as_column().unwrap();
                for (min_scalar, max_scalar) in min_column.iter().zip(max_column.iter()) {
                    let gte_min = ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "gte".to_string(),
                        params: vec![],
                        arguments: vec![
                            target_side_expr.clone(),
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span: None,
                                value: min_scalar.to_owned(),
                            }),
                        ],
                    });
                    let lte_max = ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "lte".to_string(),
                        params: vec![],
                        arguments: vec![
                            target_side_expr.clone(),
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span: None,
                                value: max_scalar.to_owned(),
                            }),
                        ],
                    });
                    let and = ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![gte_min, lte_max],
                    });
                    filter_parts.push(and);
                }
            }
            let mut or = filter_parts[0].clone();
            for filter_part in filter_parts.iter().skip(1) {
                or = ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "or".to_string(),
                    params: vec![],
                    arguments: vec![or, filter_part.clone()],
                });
            }
            filters.push(or);
        }
        let mut target_plan = m_join.target_sexpr.clone();
        Self::push_down_filters(&mut target_plan, &filters)?;
        let new_sexpr =
            join.replace_children(vec![Arc::new(target_plan), Arc::new(source_plan.clone())]);
        Ok(Box::new(new_sexpr))
    }

    fn display_scalar_expr(s: &ScalarExpr) -> String {
        match s {
            ScalarExpr::BoundColumnRef(x) => x.column.column_name.clone(),
            ScalarExpr::ConstantExpr(x) => x.value.to_string(),
            ScalarExpr::WindowFunction(x) => format!("{:?}", x),
            ScalarExpr::AggregateFunction(x) => format!("{:?}", x),
            ScalarExpr::LambdaFunction(x) => format!("{:?}", x),
            ScalarExpr::FunctionCall(x) => match x.func_name.as_str() {
                "and" | "or" | "gte" | "lte" => {
                    format!(
                        "({} {} {})",
                        Self::display_scalar_expr(&x.arguments[0]),
                        x.func_name,
                        Self::display_scalar_expr(&x.arguments[1])
                    )
                }
                _ => format!("{:?}", x),
            },
            ScalarExpr::CastExpr(x) => format!("{:?}", x),
            ScalarExpr::SubqueryExpr(x) => format!("{:?}", x),
            ScalarExpr::UDFServerCall(x) => format!("{:?}", x),
        }
    }

    fn push_down_filters(s_expr: &mut SExpr, filters: &[ScalarExpr]) -> Result<()> {
        match s_expr.plan() {
            RelOperator::Scan(s) => {
                let mut new_scan = s.clone();
                info!("push down {} filters:", filters.len());
                for filter in filters {
                    info!("{}", Self::display_scalar_expr(filter));
                }
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
            RelOperator::AddRowNumber(_) => {}
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
