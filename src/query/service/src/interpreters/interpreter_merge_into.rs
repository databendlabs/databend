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
use std::u64::MAX;

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_expression::ROW_NUMBER_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::binder::MergeIntoType;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MergeInto;
use databend_common_sql::executor::physical_plans::MergeIntoAppendNotMatched;
use databend_common_sql::executor::physical_plans::MergeIntoSource;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans;
use databend_common_sql::plans::MergeInto as MergePlan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::UpdatePlan;
use databend_common_sql::IndexType;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::meta::TableSnapshot;
use itertools::Itertools;

use crate::interpreters::common::build_update_stream_meta_seq;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::DataBlockStream;

// predicate_index should not be conflict with update expr's column_binding's index.
pub const PREDICATE_COLUMN_INDEX: IndexType = MAX as usize;
const DUMMY_COL_INDEX: usize = MAX as usize;
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
        let (physical_plan, _) = self.build_physical_plan().await?;
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan, false)
                .await?;

        // Add table lock before execution.
        // todo!(@zhyass) :But for now the lock maybe exist problem, let's open this after fix it.
        // let table_lock = LockManager::create_table_lock(table_info)?;
        // let lock_guard = table_lock.try_lock(self.ctx.clone()).await?;
        // build_res.main_pipeline.add_lock_guard(lock_guard);

        // Execute hook.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog.clone(),
                self.plan.database.clone(),
                self.plan.table.clone(),
                "merge_into".to_owned(),
                true,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = self.get_merge_into_table_result()?;
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
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
            merge_type,
            distributed,
            change_join_order,
            ..
        } = &self.plan;

        // check mutability
        let check_table = self.ctx.get_table(catalog, database, table_name).await?;
        check_table.check_mutable()?;
        // check change tracking
        if check_table.change_tracking_enabled() {
            return Err(ErrorCode::Unimplemented(format!(
                "change tracking is enabled for table '{}', does not support MERGE INTO",
                check_table.name(),
            )));
        }

        let update_stream_meta = build_update_stream_meta_seq(self.ctx.clone(), meta_data).await?;

        let table_name = table_name.clone();
        let input = input.clone();

        // we need to extract join plan, but we need to give this exchange
        // back at last.
        let (input, extract_exchange) = if let RelOperator::Exchange(_) = input.plan() {
            (Box::new(input.child(0)?.clone()), true)
        } else {
            (input, false)
        };

        let mut builder = PhysicalPlanBuilder::new(meta_data.clone(), self.ctx.clone(), false);
        // build source for MergeInto
        let join_input = builder.build(&input, *columns_set.clone()).await?;

        // find row_id column index
        let join_output_schema = join_input.output_schema()?;

        let insert_only = matches!(merge_type, MergeIntoType::InsertOnly);

        let mut row_id_idx = if !insert_only {
            match meta_data
                .read()
                .row_id_index_by_table_index(*target_table_idx)
            {
                None => {
                    return Err(ErrorCode::InvalidRowIdIndex(
                        "can't get internal row_id_idx when running merge into",
                    ));
                }
                Some(row_id_idx) => row_id_idx,
            }
        } else {
            DUMMY_COL_INDEX
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

        if *distributed && !*change_join_order {
            row_number_idx = Some(join_output_schema.index_of(ROW_NUMBER_COL_NAME)?);
        }

        if !insert_only && !found_row_id {
            // we can't get row_id_idx, throw an exception
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_id_idx when running merge into",
            ));
        }

        if *distributed && row_number_idx.is_none() && !*change_join_order {
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_number_idx when running merge into",
            ));
        }

        let table = self.ctx.get_table(catalog, database, &table_name).await?;
        let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support MERGE INTO",
                table.name(),
                table.get_table_info().engine(),
            ))
        })?;

        let table_info = fuse_table.get_table_info().clone();
        let catalog_ = self.ctx.get_catalog(catalog).await?;

        // merge_into_source is used to recv join's datablocks and split them into macthed and not matched
        // datablocks.
        let merge_into_source = if !*distributed && extract_exchange {
            // if we doesn't support distributed merge into, we should give the exchange merge back.
            let rollback_join_input = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(join_input),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            });
            PhysicalPlan::MergeIntoSource(MergeIntoSource {
                input: Box::new(rollback_join_input),
                row_id_idx: row_id_idx as u32,
                merge_type: merge_type.clone(),
            })
        } else {
            PhysicalPlan::MergeIntoSource(MergeIntoSource {
                input: Box::new(join_input),
                row_id_idx: row_id_idx as u32,
                merge_type: merge_type.clone(),
            })
        };

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
                        Some(alias) => alias.name.to_string().to_lowercase(),
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
        let target_build_optimization =
            matches!(self.plan.merge_type, MergeIntoType::FullOperation)
                && !self.plan.columns_set.contains(&self.plan.row_id_index);
        let commit_input = if !distributed {
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
                merge_type: merge_type.clone(),
                change_join_order: *change_join_order,
                target_build_optimization,
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
                segments: segments.clone(),
                distributed: true,
                output_schema: match *change_join_order {
                    false => DataSchemaRef::new(DataSchema::new(vec![
                        join_output_schema.fields[row_number_idx.unwrap()].clone(),
                    ])),
                    true => DataSchemaRef::new(DataSchema::new(vec![DataField::new(
                        ROW_ID_COL_NAME,
                        databend_common_expression::types::DataType::Number(
                            databend_common_expression::types::NumberDataType::UInt64,
                        ),
                    )])),
                },
                merge_type: merge_type.clone(),
                change_join_order: *change_join_order,
                target_build_optimization: false, // we don't support for distributed mode for now..
            }));
            // if change_join_order = true, it means the target is build side,
            // in this way, we will do matched operation and not matched operation
            // locally in every node, and the main node just receive rowids to apply.
            let segments = if *change_join_order {
                segments.clone()
            } else {
                vec![]
            };
            PhysicalPlan::MergeIntoAppendNotMatched(Box::new(MergeIntoAppendNotMatched {
                input: Box::new(PhysicalPlan::Exchange(Exchange {
                    plan_id: 0,
                    input: Box::new(merge_append),
                    kind: FragmentKind::Merge,
                    keys: vec![],
                    allow_adjust_parallelism: true,
                    ignore_exchange: false,
                })),
                table_info: table_info.clone(),
                catalog_info: catalog_.info(),
                unmatched: unmatched.clone(),
                input_schema: merge_into_source.output_schema()?,
                merge_type: merge_type.clone(),
                change_join_order: *change_join_order,
                segments,
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
            update_stream_meta: update_stream_meta.clone(),
            merge_meta: false,
            need_lock: false,
            deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
        }));

        Ok((physical_plan, table_info))
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

    fn get_merge_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let binding = self.ctx.get_merge_status();
        let status = binding.read();
        let schema = self.plan.schema();
        let mut columns = Vec::new();
        for field in schema.as_ref().fields() {
            match field.name().as_str() {
                plans::INSERT_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.insert_rows as u32]))
                }
                plans::UPDATE_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.update_rows as u32]))
                }
                plans::DELETE_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.deleted_rows as u32]))
                }
                _ => unreachable!(),
            }
        }
        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}
