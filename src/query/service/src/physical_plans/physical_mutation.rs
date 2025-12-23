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

use std::any::Any;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::NUM_ROW_ID_PREFIX_BITS;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::PREDICATE_COLUMN_NAME;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::expr::*;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ColumnEntry;
use databend_common_sql::ColumnSet;
use databend_common_sql::DUMMY_COLUMN_INDEX;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;
use databend_common_sql::Visibility;
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::binder::MutationType;
use databend_common_sql::binder::wrap_cast;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::parse_computed_expr;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::TruncateMode;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::NUM_BLOCK_ID_BITS;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use itertools::Itertools;
use tokio::sync::Semaphore;

use super::ColumnMutation;
use super::CommitType;
use crate::physical_plans::CommitSink;
use crate::physical_plans::Exchange;
use crate::physical_plans::MutationManipulate;
use crate::physical_plans::MutationOrganize;
use crate::physical_plans::MutationSplit;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::RowFetch;
use crate::physical_plans::format::MutationFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

// The predicate_column_index should not be conflict with update expr's column_binding's index.
pub const PREDICATE_COLUMN_INDEX: IndexType = u64::MAX as usize;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Mutation {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub table_info: TableInfo,
    // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
    pub unmatched: Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>,
    pub segments: Vec<(usize, Location)>,
    pub strategy: MutationStrategy,
    pub target_table_index: usize,
    pub need_match: bool,
    pub distributed: bool,
    pub target_build_optimization: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for Mutation {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(MutationFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Mutation {
            meta: self.meta.clone(),
            input,
            table_info: self.table_info.clone(),
            unmatched: self.unmatched.clone(),
            segments: self.segments.clone(),
            strategy: self.strategy.clone(),
            target_table_index: self.target_table_index,
            need_match: self.need_match,
            distributed: self.distributed,
            target_build_optimization: self.target_build_optimization,
            table_meta_timestamps: self.table_meta_timestamps,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let tbl = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;

        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();

        let cluster_stats_gen =
            table.get_cluster_stats_gen(builder.ctx.clone(), 0, block_thresholds, None)?;

        let max_threads = builder.settings.get_max_threads()? as usize;
        let io_request_semaphore = Arc::new(Semaphore::new(max_threads));

        // For row_id port, create rowid_aggregate_mutator
        // For matched data port and unmatched port, do serialize
        let serialize_len = match self.strategy {
            MutationStrategy::NotMatchedOnly => builder.main_pipeline.output_len(),
            MutationStrategy::MixedMatched | MutationStrategy::MatchedOnly => {
                // remove row id port
                builder.main_pipeline.output_len() - 1
            }
            MutationStrategy::Direct => unreachable!(),
        };

        // 1. Fill default and computed columns
        builder.build_fill_columns_in_merge_into(
            tbl.clone(),
            serialize_len,
            self.need_match,
            self.unmatched.clone(),
        )?;

        // 2. Add cluster‘s blocksort if it's a cluster table
        builder.build_compact_and_cluster_sort_in_merge_into(
            table,
            self.need_match,
            serialize_len,
            block_thresholds,
        )?;

        let mut pipe_items = Vec::with_capacity(builder.main_pipeline.output_len());

        // 3.1 Add rowid_aggregate_mutator for row_id port
        if self.need_match {
            pipe_items.push(table.rowid_aggregate_mutator(
                builder.ctx.clone(),
                cluster_stats_gen.clone(),
                io_request_semaphore,
                self.segments.clone(),
                false,
                self.table_meta_timestamps,
            )?);
        }

        // 3.2 Add serialize_block_transform for data port
        for _ in 0..serialize_len {
            let serialize_block_transform = TransformSerializeBlock::try_create(
                builder.ctx.clone(),
                InputPort::create(),
                OutputPort::create(),
                table,
                cluster_stats_gen.clone(),
                MutationKind::MergeInto,
                self.table_meta_timestamps,
            )?;
            pipe_items.push(serialize_block_transform.into_pipe_item());
        }

        let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
        builder.main_pipeline.add_pipe(Pipe::create(
            builder.main_pipeline.output_len(),
            output_len,
            pipe_items,
        ));

        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_mutation(
        &mut self,
        s_expr: &SExpr,
        mutation: &databend_common_sql::plans::Mutation,
        mut required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        let databend_common_sql::plans::Mutation {
            bind_context,
            metadata,
            catalog_name,
            database_name,
            table_name,
            table_name_alias,
            matched_evaluators,
            unmatched_evaluators,
            mutation_type,
            target_table_index,
            field_index_map,
            strategy,
            distributed,
            predicate_column_index,
            row_id_index,
            row_id_shuffle,
            can_try_update_column_only,
            no_effect,
            truncate_table,
            direct_filter,
            ..
        } = mutation;

        let mut maybe_udfs = BTreeSet::new();
        for matched_evaluator in matched_evaluators {
            if let Some(condition) = &matched_evaluator.condition {
                maybe_udfs.extend(condition.used_columns());
            }
            if let Some(update_list) = &matched_evaluator.update {
                for update_scalar in update_list.values() {
                    maybe_udfs.extend(update_scalar.used_columns());
                }
            }
        }
        for unmatched_evaluator in unmatched_evaluators {
            if let Some(condition) = &unmatched_evaluator.condition {
                maybe_udfs.extend(condition.used_columns());
            }
            for value in &unmatched_evaluator.values {
                maybe_udfs.extend(value.used_columns());
            }
        }
        for filter_value in direct_filter {
            maybe_udfs.extend(filter_value.used_columns());
        }

        let udf_ids = s_expr.get_udfs_col_ids()?;
        let required_udf_ids: BTreeSet<_> = maybe_udfs.intersection(&udf_ids).collect();
        let udf_col_num = required_udf_ids.len();
        required.extend(required_udf_ids);

        let mut plan = self.build(s_expr.child(0)?, required).await?;
        if *no_effect {
            return Ok(plan);
        }

        let table = self
            .ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;
        let table_info = table.get_table_info();
        let table_name = table_name.clone();

        let mutation_build_info = self.mutation_build_info.clone().unwrap();
        let mutation_input_schema = plan.output_schema()?;

        if *truncate_table {
            // Do truncate.
            plan = PhysicalPlan::new(CommitSink {
                input: plan,
                snapshot: mutation_build_info.table_snapshot,
                table_info: table_info.clone(),
                // let's use update first, we will do some optimizations and select exact strategy
                commit_type: CommitType::Truncate {
                    mode: TruncateMode::Delete,
                },
                update_stream_meta: vec![],
                deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                recluster_info: None,
                meta: PhysicalPlanMeta::new("CommitSink"),
                table_meta_timestamps: mutation_build_info.table_meta_timestamps,
            });
            plan.adjust_plan_id(&mut 0);
            return Ok(plan);
        }

        if *strategy == MutationStrategy::Direct {
            // MutationStrategy::Direct: If the mutation filter is a simple expression,
            // we use MutationSource to execute the mutation directly.
            let mut field_id_to_schema_index = HashMap::new();
            let (mutation_expr, computed_expr, mutation_kind) =
                if let Some(update_list) = &matched_evaluators[0].update {
                    let (database, table_name) = match table_name_alias {
                        None => (Some(database_name.as_str()), table_name.clone()),
                        Some(table_name_alias) => (None, table_name_alias.to_lowercase()),
                    };
                    let mutation_expr = mutation_update_expr(
                        self.ctx.clone(),
                        bind_context,
                        update_list,
                        table.schema_with_stream().into(),
                        mutation_input_schema.clone(),
                        *predicate_column_index,
                        database,
                        &table_name,
                    )?;

                    build_field_id_to_schema_index(
                        database,
                        &table_name,
                        bind_context,
                        table.schema_with_stream(),
                        mutation_input_schema.clone(),
                        &mut field_id_to_schema_index,
                    );

                    let computed_expr = generate_stored_computed_list(
                        self.ctx.clone(),
                        bind_context,
                        Arc::new(table.schema().into()),
                        mutation_input_schema.clone(),
                        database,
                        &table_name,
                        update_list,
                    )?;

                    (
                        Some(mutation_expr),
                        Some(computed_expr),
                        MutationKind::Update,
                    )
                } else {
                    (None, None, MutationKind::Delete)
                };

            plan = PhysicalPlan::new(ColumnMutation {
                input: plan,
                meta: PhysicalPlanMeta::new("ColumnMutation"),
                table_info: mutation_build_info.table_info.clone(),
                mutation_expr,
                computed_expr,
                mutation_kind,
                field_id_to_schema_index,
                input_num_columns: mutation_input_schema.fields().len(),
                has_filter_column: predicate_column_index.is_some(),
                table_meta_timestamps: mutation_build_info.table_meta_timestamps,
                udf_col_num,
            });

            if *distributed {
                plan = PhysicalPlan::new(Exchange {
                    input: plan,
                    kind: FragmentKind::Merge,
                    keys: vec![],
                    allow_adjust_parallelism: true,
                    ignore_exchange: false,
                    meta: PhysicalPlanMeta::new("Exchange"),
                });
            }

            plan = PhysicalPlan::new(CommitSink {
                input: plan,
                snapshot: mutation_build_info.table_snapshot,
                table_info: table_info.clone(),
                // let's use update first, we will do some optimizations and select exact strategy
                commit_type: CommitType::Mutation {
                    kind: mutation_kind,
                    merge_meta: false,
                },
                update_stream_meta: vec![],
                deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                meta: PhysicalPlanMeta::new("CommitSink"),
                recluster_info: None,
                table_meta_timestamps: mutation_build_info.table_meta_timestamps,
            });

            plan.adjust_plan_id(&mut 0);
            return Ok(plan);
        }

        let is_not_matched_only = matches!(strategy, MutationStrategy::NotMatchedOnly);
        let row_id_offset = if !is_not_matched_only {
            mutation_input_schema.index_of(&row_id_index.to_string())?
        } else {
            DUMMY_COLUMN_INDEX
        };

        // For distributed merge, we shuffle data blocks by block_id (derived from row_id) to avoid
        // different nodes update the same physical block simultaneously, data blocks that are needed
        // to insert just keep in local node.
        if *distributed && *row_id_shuffle && !is_not_matched_only {
            plan = build_block_id_shuffle_exchange(
                plan,
                bind_context,
                mutation_input_schema.clone(),
                database_name,
                &table_name,
            )?;
        }

        // If the mutation type is FullOperation, we use row_id column to split a block
        // into matched and not matched parts.
        if matches!(strategy, MutationStrategy::MixedMatched) {
            plan = PhysicalPlan::new(MutationSplit {
                input: plan,
                split_index: row_id_offset,
                meta: PhysicalPlanMeta::new("MutationSplit"),
            });
        }

        // Construct row fetch plan for lazy columns.
        if let Some(lazy_columns) = self
            .metadata
            .read()
            .get_table_lazy_columns(target_table_index)
            && !lazy_columns.is_empty()
        {
            plan = build_mutation_row_fetch(
                plan,
                metadata.clone(),
                mutation_input_schema.clone(),
                strategy.clone(),
                lazy_columns.clone(),
                *target_table_index,
                row_id_offset,
            );
        }

        let output_schema = plan.output_schema()?;

        // transform unmatched for insert
        // reference to func `build_eval_scalar`
        // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
        let mut unmatched =
            Vec::<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>::with_capacity(
                unmatched_evaluators.len(),
            );

        for item in unmatched_evaluators {
            let filter = if let Some(filter_expr) = &item.condition {
                Some(self.scalar_expr_to_remote_expr(filter_expr, output_schema.clone())?)
            } else {
                None
            };

            let mut values_exprs = Vec::<RemoteExpr>::with_capacity(item.values.len());

            for scalar_expr in &item.values {
                values_exprs
                    .push(self.scalar_expr_to_remote_expr(scalar_expr, output_schema.clone())?)
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
                    .scalar_expr_to_remote_expr(condition, output_schema.clone())?
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

            if *can_try_update_column_only {
                assert!(condition.is_none());
            }

            // update
            let update_list = if let Some(update_list) = &item.update {
                // we don't need real col_indices here, just give a
                // dummy index, that's ok.
                let col_indices = vec![DUMMY_COLUMN_INDEX];
                let (database, table_name) = match table_name_alias {
                    None => (Some(database_name.as_str()), table_name.clone()),
                    Some(table_name_alias) => (None, table_name_alias.to_lowercase()),
                };
                let update_list = generate_update_list(
                    self.ctx.clone(),
                    bind_context,
                    update_list,
                    table.schema_with_stream().into(),
                    col_indices,
                    Some(PREDICATE_COLUMN_INDEX),
                    database,
                    &table_name,
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
                                    // so it's not in mutation_input_schema for now. But it's must be added
                                    // to the tail, so let do it like below.
                                    if *name == PREDICATE_COLUMN_INDEX.to_string() {
                                        Ok(output_schema.num_fields())
                                    } else {
                                        output_schema.index_of(name)
                                    }
                                })
                                .unwrap()
                                .as_remote_expr(),
                        )
                    })
                    .collect_vec();
                // update
                Some(update_list)
            } else {
                // delete
                None
            };
            matched.push((condition, update_list))
        }

        let mut field_index_of_input_schema = HashMap::<FieldIndex, usize>::new();
        for (field_index, value) in field_index_map {
            // Safe to set field index, to fix issue #16588.
            if let Ok(value) = output_schema.index_of(value) {
                field_index_of_input_schema.insert(*field_index, value);
            }
        }

        plan = PhysicalPlan::new(MutationManipulate {
            input: plan,
            table_info: table_info.clone(),
            unmatched: unmatched.clone(),
            matched: matched.clone(),
            field_index_of_input_schema: field_index_of_input_schema.clone(),
            strategy: strategy.clone(),
            row_id_idx: row_id_offset,
            can_try_update_column_only: *can_try_update_column_only,
            unmatched_schema: mutation_input_schema.clone(),
            meta: PhysicalPlanMeta::new("MutationManipulate"),
            target_table_index: *target_table_index,
        });

        plan = PhysicalPlan::new(MutationOrganize {
            input: plan,
            strategy: strategy.clone(),
            meta: PhysicalPlanMeta::new("MutationOrganize"),
        });

        let segments: Vec<_> = mutation_build_info
            .table_snapshot
            .segments()
            .iter()
            .cloned()
            .enumerate()
            .collect();

        plan = PhysicalPlan::new(Mutation {
            input: plan,
            table_info: table_info.clone(),
            unmatched,
            segments: segments.clone(),
            distributed: *distributed,
            strategy: strategy.clone(),
            target_table_index: *target_table_index,
            need_match: !is_not_matched_only,
            target_build_optimization: false,
            meta: PhysicalPlanMeta::new("Mutation"),
            table_meta_timestamps: mutation_build_info.table_meta_timestamps,
        });

        if *distributed {
            plan = PhysicalPlan::new(Exchange {
                input: plan,
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
                meta: PhysicalPlanMeta::new("Exchange"),
            });
        }

        let mutation_kind = match mutation_type {
            MutationType::Update | MutationType::Merge => MutationKind::Update,
            MutationType::Delete => MutationKind::Delete,
        };

        // build mutation_aggregate
        let mut physical_plan = PhysicalPlan::new(CommitSink {
            input: plan,
            snapshot: mutation_build_info.table_snapshot,
            table_info: table_info.clone(),
            // let's use update first, we will do some optimizations and select exact strategy
            commit_type: CommitType::Mutation {
                kind: mutation_kind,
                merge_meta: false,
            },
            update_stream_meta: mutation_build_info.update_stream_meta,
            deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            recluster_info: None,
            meta: PhysicalPlanMeta::new("CommitSink"),
            table_meta_timestamps: mutation_build_info.table_meta_timestamps,
        });

        physical_plan.adjust_plan_id(&mut 0);
        Ok(physical_plan)
    }

    fn scalar_expr_to_remote_expr(
        &self,
        scalar_expr: &ScalarExpr,
        schema: DataSchemaRef,
    ) -> Result<RemoteExpr> {
        let scalar_expr = scalar_expr
            .type_check(schema.as_ref())?
            .project_column_ref(|index| schema.index_of(&index.to_string()))?;
        let (filer, _) = ConstantFolder::fold(
            &scalar_expr,
            &self.ctx.get_function_context().unwrap(),
            &BUILTIN_FUNCTIONS,
        );
        Ok(filer.as_remote_expr())
    }
}

pub fn build_block_id_shuffle_exchange(
    plan: PhysicalPlan,
    bind_context: &BindContext,
    mutation_input_schema: Arc<DataSchema>,
    database_name: &str,
    table_name: &str,
) -> Result<PhysicalPlan> {
    let mut row_id_column = None;
    for column_binding in bind_context.columns.iter() {
        if BindContext::match_column_binding(
            Some(database_name),
            Some(table_name),
            ROW_ID_COL_NAME,
            column_binding,
        ) {
            row_id_column = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: column_binding.clone(),
            }));
            break;
        }
    }
    let row_id_column = row_id_column.ok_or_else(|| ErrorCode::Internal("It's a bug"))?;

    let row_id_expr = row_id_column
        .type_check(mutation_input_schema.as_ref())?
        .project_column_ref(|index| mutation_input_schema.index_of(&index.to_string()))?;

    let block_id_shuffle_key = check_function(
        None,
        "bit_and",
        &[],
        &[
            check_function(
                None,
                "bit_shift_right",
                &[],
                &[
                    row_id_expr,
                    Expr::Constant(Constant {
                        span: None,
                        scalar: Scalar::Number(((64 - NUM_ROW_ID_PREFIX_BITS) as u64).into()),
                        data_type: DataType::Number(NumberDataType::UInt64),
                    }),
                ],
                &BUILTIN_FUNCTIONS,
            )?,
            Expr::Constant(Constant {
                span: None,
                scalar: Scalar::Number((((1 << NUM_BLOCK_ID_BITS) - 1) as u64).into()),
                data_type: DataType::Number(NumberDataType::UInt64),
            }),
        ],
        &BUILTIN_FUNCTIONS,
    )?;

    Ok(PhysicalPlan::new(Exchange {
        input: plan,
        kind: FragmentKind::Normal,
        meta: PhysicalPlanMeta::new("Exchange"),
        keys: vec![block_id_shuffle_key.as_remote_expr()],
        allow_adjust_parallelism: true,
        ignore_exchange: false,
    }))
}

fn build_mutation_row_fetch(
    plan: PhysicalPlan,
    metadata: MetadataRef,
    mutation_input_schema: Arc<DataSchema>,
    strategy: MutationStrategy,
    lazy_columns: ColumnSet,
    target_table_index: usize,
    row_id_offset: usize,
) -> PhysicalPlan {
    let metadata = metadata.read();

    let lazy_columns = lazy_columns
        .iter()
        .filter(|index| !mutation_input_schema.has_field(&index.to_string())) // If the column is already in the input schema, we don't need to fetch it.
        .cloned()
        .collect::<Vec<_>>();
    let mut has_inner_column = false;
    let need_wrap_nullable = matches!(strategy, MutationStrategy::MixedMatched);
    let fetched_fields: Vec<DataField> = lazy_columns
        .iter()
        .map(|index| {
            let col = metadata.column(*index);
            if let ColumnEntry::BaseTableColumn(c) = col {
                if c.path_indices.is_some() {
                    has_inner_column = true;
                }
            }
            let mut data_type = col.data_type();
            if need_wrap_nullable {
                data_type = data_type.wrap_nullable();
            }
            DataField::new(&index.to_string(), data_type)
        })
        .collect();

    let source = metadata
        .get_table_source(&target_table_index)
        .unwrap()
        .clone();
    let table_schema = source.source_info.schema();
    let cols_to_fetch = PhysicalPlanBuilder::build_projection(
        &metadata,
        &table_schema,
        lazy_columns.iter(),
        has_inner_column,
        true,
        true,
    );

    PhysicalPlan::new(RowFetch {
        input: plan,
        source: Box::new(source),
        row_id_col_offset: row_id_offset,
        cols_to_fetch,
        fetched_fields,
        need_wrap_nullable,
        stat_info: None,
        meta: PhysicalPlanMeta::new("RowFetch"),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn generate_update_list(
    ctx: Arc<dyn TableContext>,
    bind_context: &BindContext,
    update_list: &HashMap<FieldIndex, ScalarExpr>,
    schema: DataSchema,
    col_indices: Vec<usize>,
    use_column_name_index: Option<usize>,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<(FieldIndex, RemoteExpr<String>)>> {
    let column = ColumnBindingBuilder::new(
        PREDICATE_COLUMN_NAME.to_string(),
        use_column_name_index.unwrap_or_else(|| schema.num_fields()),
        Box::new(DataType::Boolean),
        Visibility::Visible,
    )
    .build();
    let predicate = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });

    update_list.iter().try_fold(
        Vec::with_capacity(update_list.len()),
        |mut acc, (index, scalar)| {
            let field = schema.field(*index);
            let data_type = scalar.data_type()?;
            let target_type = field.data_type();

            let scalar = if col_indices.is_empty() {
                // The condition is always true.
                // Replace column to the result of the following expression:
                // CAST(expression, type)
                if data_type != *target_type {
                    wrap_cast(scalar, target_type)
                } else {
                    scalar.clone()
                }
            } else {
                // Replace column to the result of the following expression:
                // if(condition, CAST(expression, type), column)
                let mut right = None;
                for column_binding in bind_context.columns.iter() {
                    if BindContext::match_column_binding(
                        database,
                        Some(table),
                        field.name(),
                        column_binding,
                    ) {
                        right = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: column_binding.clone(),
                        }));
                        break;
                    }
                }

                let right = right.ok_or_else(|| {
                    ErrorCode::Internal(
                        format!("Can not find column {} in table {}", field.name(), table)
                            .to_string(),
                    )
                })?;

                // If right is nullable, left must also be wrapped in nullable to ensure both have the same type.
                let target_type = if right.data_type()?.is_nullable() {
                    target_type.wrap_nullable()
                } else {
                    target_type.clone()
                };
                let left = if data_type != target_type {
                    wrap_cast(scalar, &target_type)
                } else {
                    scalar.clone()
                };

                // corner case: for merge into, if target_table's fields are not null, when after bind_join, it will
                // change into nullable, so we need to cast this. but we will do cast after all matched clauses,please
                // see `cast_data_type_for_merge()`.

                ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "if".to_string(),
                    params: vec![],
                    arguments: vec![predicate.clone(), left, right],
                })
            };
            let expr = scalar.as_expr()?.project_column_ref(|col| {
                if use_column_name_index.is_none() {
                    Ok(col.column_name.clone())
                } else {
                    Ok(col.index.to_string())
                }
            })?;
            let (expr, _) =
                ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
            acc.push((*index, expr.as_remote_expr()));
            Ok::<_, ErrorCode>(acc)
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub fn mutation_update_expr(
    ctx: Arc<dyn TableContext>,
    bind_context: &BindContext,
    update_list: &HashMap<FieldIndex, ScalarExpr>,
    schema: DataSchema,
    input_schema: Arc<DataSchema>,
    predicate_column_index: Option<usize>,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<(FieldIndex, RemoteExpr)>> {
    let predicate = if let Some(predicate_column_index) = predicate_column_index {
        let column = ColumnBindingBuilder::new(
            PREDICATE_COLUMN_NAME.to_string(),
            predicate_column_index,
            Box::new(DataType::Boolean),
            Visibility::Visible,
        )
        .build();
        ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column })
    } else {
        ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Boolean(true),
        })
    };

    update_list.iter().try_fold(
        Vec::with_capacity(update_list.len()),
        |mut acc, (index, scalar)| {
            let field = schema.field(*index);
            let data_type = scalar.data_type()?;
            let target_type = field.data_type();

            // Replace column to the result of the following expression:
            // if(condition, CAST(expression, type), column)
            let mut right = None;
            for column_binding in bind_context.columns.iter() {
                if BindContext::match_column_binding(
                    database,
                    Some(table),
                    field.name(),
                    column_binding,
                ) {
                    right = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: column_binding.clone(),
                    }));
                    break;
                }
            }
            let right = right.ok_or_else(|| {
                ErrorCode::Internal(
                    format!("Can not find column {} in table {}", field.name(), table).to_string(),
                )
            })?;

            // If right is nullable, left must also be wrapped in nullable to ensure both have the same type.
            let target_type = if right.data_type()?.is_nullable() {
                target_type.wrap_nullable()
            } else {
                target_type.clone()
            };
            let left = if data_type != target_type {
                wrap_cast(scalar, &target_type)
            } else {
                scalar.clone()
            };

            let scalar = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "if".to_string(),
                params: vec![],
                arguments: vec![predicate.clone(), left, right],
            });
            let expr = scalar
                .type_check(input_schema.as_ref())?
                .project_column_ref(|index| input_schema.index_of(&index.to_string()))?;
            let (expr, _) =
                ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
            acc.push((*index, expr.as_remote_expr()));
            Ok::<_, ErrorCode>(acc)
        },
    )
}

pub fn generate_stored_computed_list(
    ctx: Arc<dyn TableContext>,
    bind_context: &BindContext,
    schema: DataSchemaRef,
    input_schema: Arc<DataSchema>,
    database: Option<&str>,
    table: &str,
    update_list: &HashMap<FieldIndex, ScalarExpr>,
) -> Result<Vec<(FieldIndex, RemoteExpr)>> {
    let mut remote_exprs = Vec::new();
    for (i, f) in schema.fields().iter().enumerate() {
        if let Some(ComputedExpr::Stored(stored_expr)) = f.computed_expr() {
            let expr = parse_computed_expr(ctx.clone(), schema.clone(), stored_expr)?;
            let expr = check_cast(None, false, expr, f.data_type(), &BUILTIN_FUNCTIONS)?;

            // If related column has updated, the stored computed column need to regenerate.
            let mut need_update = false;
            let field_indices = expr.column_refs();
            for (field_index, _) in field_indices.iter() {
                if update_list.contains_key(field_index) {
                    need_update = true;
                    break;
                }
            }
            if need_update {
                let expr = expr.project_column_ref(|id| {
                    let mut column_index: Option<usize> = None;
                    for column_binding in bind_context.columns.iter() {
                        if BindContext::match_column_binding(
                            database,
                            Some(table),
                            schema.field(*id).name(),
                            column_binding,
                        ) {
                            column_index = Some(column_binding.index);
                            break;
                        }
                    }
                    input_schema.index_of(&column_index.unwrap().to_string())
                })?;
                let (expr, _) =
                    ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
                remote_exprs.push((i, expr.as_remote_expr()));
            }
        }
    }
    Ok(remote_exprs)
}

fn build_field_id_to_schema_index(
    database: Option<&str>,
    table_name: &str,
    bind_context: &BindContext,
    table_schema_with_stream: Arc<TableSchema>,
    mutation_input_schema: Arc<DataSchema>,
    field_id_to_schema_index: &mut HashMap<usize, usize>,
) {
    for (field_id, field) in table_schema_with_stream.fields().iter().enumerate() {
        if matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_))) {
            continue;
        }
        for column_binding in bind_context.columns.iter() {
            if BindContext::match_column_binding(
                database,
                Some(table_name),
                field.name(),
                column_binding,
            ) {
                let column_index = column_binding.index;
                let schema_index = mutation_input_schema
                    .index_of(&column_index.to_string())
                    .unwrap();
                field_id_to_schema_index.insert(field_id, schema_index);
                break;
            }
        }
    }
}
