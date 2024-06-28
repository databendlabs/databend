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
use std::u64::MAX;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_expression::ROW_NUMBER_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::Location;
use itertools::Itertools;

use crate::binder::MergeIntoType;
use crate::executor::physical_plan::PhysicalPlan;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::MergeIntoManipulate;
use crate::executor::physical_plans::MergeIntoOrganize;
use crate::executor::physical_plans::MergeIntoSerialize;
use crate::executor::physical_plans::MergeIntoSplit;
use crate::executor::physical_plans::MutationKind;
use crate::executor::physical_plans::RowFetch;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::plans;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;
use crate::TypeCheck;
use crate::DUMMY_COLUMN_INDEX;

// predicate_index should not be conflict with update expr's column_binding's index.
pub const PREDICATE_COLUMN_INDEX: IndexType = MAX as usize;
pub type MatchExpr = Vec<(Option<RemoteExpr>, Option<Vec<(FieldIndex, RemoteExpr)>>)>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MergeInto {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub table_info: TableInfo,
    // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
    pub unmatched: Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>,
    pub output_schema: DataSchemaRef,
    pub merge_into_op: MergeIntoOp,
    pub need_match: bool,
    pub distributed: bool,
    pub change_join_order: bool,
    pub target_build_optimization: bool,
    pub enable_right_broadcast: bool,
}

impl PhysicalPlanBuilder {
    // MergeInto strategies:
    // 1. target_build_optimization, this is enabled in standalone mode and in this case we don't need rowid column anymore.
    // but we just support for `merge into xx using source on xxx when matched then update xxx when not matched then insert xxx`.
    // 2. merge into join strategies:
    // Left,Right,Inner,Left Anti, Right Anti
    // important flag:
    //      I. change join order: if true, target table as build side, if false, source as build side.
    //      II. distributed: this merge into is executed at a distributed stargety.
    // 2.1 Left: there are matched and not matched, and change join order is true.
    // 2.2 Left Anti: change join order is true, but it's insert-only.
    // 2.3 Inner: this is matched only case.
    //      2.3.1 change join order is true, target table as build side,it's matched-only.
    //      2.3.2 change join order is false, source data as build side,it's matched-only.
    // 2.4 Right: change join order is false, there are matched and not matched
    // 2.5 Right Anti: change join order is false, but it's insert-only.
    // distributed execution stargeties:
    // I. change join order is true, we use the `optimize_distributed_query`'s result.
    // II. change join order is false and match_pattern and not enable spill, we use right outer join with rownumber distributed strategies.
    // III otherwise, use `merge_into_join_sexpr` as standalone execution(so if change join order is false,but doesn't match_pattern, we don't support distributed,in fact. case I
    // can take this at most time, if that's a hash shuffle, the I can take it. We think source is always very small).
    pub async fn build_merge_into(
        &mut self,
        s_expr: &SExpr,
        merge_into: &crate::plans::MergeInto,
    ) -> Result<PhysicalPlan> {
        let crate::plans::MergeInto {
            bind_context,
            meta_data,
            columns_set,
            catalog,
            database,
            table: table_name,
            target_alias,
            matched_evaluators,
            unmatched_evaluators,
            target_table_index,
            field_index_map,
            merge_type,
            distributed,
            change_join_order,
            row_id_index,
            source_row_id_index,
            can_try_update_column_only,
            enable_right_broadcast,
            lazy_columns,
            ..
        } = merge_into;

        let mut columns_set = if let Some(lazy_columns) = lazy_columns {
            columns_set
                .difference(lazy_columns)
                .cloned()
                .collect::<ColumnSet>()
        } else {
            *columns_set.clone()
        };

        let mut builder = PhysicalPlanBuilder::new(meta_data.clone(), self.ctx.clone(), false);
        let mut plan = builder.build(s_expr.child(0)?, columns_set.clone()).await?;

        let join_output_schema = plan.output_schema()?;
        let is_insert_only = matches!(merge_type, MergeIntoType::InsertOnly);
        if !is_insert_only && !join_output_schema.has_field(&row_id_index.to_string()) {
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get row_id_index when running merge into",
            ));
        }

        let row_id_offset = if !is_insert_only {
            columns_set.insert(*row_id_index);
            join_output_schema.index_of(&row_id_index.to_string())?
        } else {
            DUMMY_COLUMN_INDEX
        };

        // We use `merge_into_split_idx` to specify a column from target table to spilt a block
        // from join into matched part and unmatched part.
        let mut merge_into_split_idx = None;
        if matches!(merge_type, MergeIntoType::FullOperation) {
            for (idx, data_field) in join_output_schema.fields().iter().enumerate() {
                if *data_field.name() == row_id_index.to_string() {
                    merge_into_split_idx = Some(idx);
                    break;
                }
            }
        }

        if let Some(merge_into_split_idx) = merge_into_split_idx {
            plan = PhysicalPlan::MergeIntoSplit(Box::new(MergeIntoSplit {
                plan_id: 0,
                input: Box::new(plan),
                split_index: merge_into_split_idx,
            }));
        }

        if let Some(lazy_columns) = lazy_columns
            && !lazy_columns.is_empty()
        {
            let row_id_offset = join_output_schema.index_of(&row_id_index.to_string())?;
            let lazy_columns = lazy_columns
                .iter()
                .sorted() // Needs sort because we need to make the order deterministic.
                .filter(|index| !join_output_schema.has_field(&index.to_string())) // If the column is already in the input schema, we don't need to fetch it.
                .cloned()
                .collect::<Vec<_>>();

            let mut has_inner_column = false;
            let need_wrap_nullable = matches!(merge_type, MergeIntoType::FullOperation);
            let fetched_fields: Vec<DataField> = lazy_columns
                .iter()
                .map(|index| {
                    let metadata = meta_data.read();
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

            let source = plan.try_find_data_source(*target_table_index);
            debug_assert!(source.is_some());
            let source_info = source.cloned().unwrap();
            let table_schema = source_info.source_info.schema();
            let cols_to_fetch = PhysicalPlanBuilder::build_projection(
                &meta_data.read(),
                &table_schema,
                lazy_columns.iter(),
                has_inner_column,
                true,
                true,
                false,
            );
            plan = PhysicalPlan::RowFetch(RowFetch {
                plan_id: 0,
                input: Box::new(plan),
                source: Box::new(source_info),
                row_id_col_offset: row_id_offset,
                cols_to_fetch,
                fetched_fields,
                need_wrap_nullable,
                stat_info: None,
            });
        }

        let output_schema = plan.output_schema()?;

        let mut source_row_id_idx = None;
        let mut source_row_number_idx = None;
        if *enable_right_broadcast {
            if let Some(source_row_id_index) = source_row_id_index {
                for (idx, data_field) in join_output_schema.fields().iter().enumerate() {
                    if *data_field.name() == source_row_id_index.to_string() {
                        source_row_id_idx = Some(idx);
                        break;
                    }
                }
            } else {
                source_row_number_idx = Some(join_output_schema.index_of(ROW_NUMBER_COL_NAME)?);
            }
        };

        if *enable_right_broadcast && source_row_number_idx.is_none() && source_row_id_idx.is_none()
        {
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_number_idx or row_id_idx when running merge into",
            ));
        }

        let table = self.ctx.get_table(catalog, database, table_name).await?;
        let table_info = table.get_table_info();
        let table_name = table_name.clone();

        // transform unmatched for insert
        // reference to func `build_eval_scalar`
        // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
        let mut unmatched =
            Vec::<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>::with_capacity(
                unmatched_evaluators.len(),
            );

        for item in unmatched_evaluators {
            let filter = if let Some(filter_expr) = &item.condition {
                Some(self.transform_scalar_expr2expr(filter_expr, output_schema.clone())?)
            } else {
                None
            };

            let mut values_exprs = Vec::<RemoteExpr>::with_capacity(item.values.len());

            for scalar_expr in &item.values {
                values_exprs
                    .push(self.transform_scalar_expr2expr(scalar_expr, output_schema.clone())?)
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
                    .transform_scalar_expr2expr(condition, output_schema.clone())?
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
                let (database, table_name) = match target_alias {
                    None => (Some(database.as_str()), table_name.clone()),
                    Some(alias) => (None, alias.name.to_string().to_lowercase()),
                };
                let update_list = plans::generate_update_list(
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
                                    // so it's not in join_output_schema for now. But it's must be added
                                    // to the tail, so let do it like below.
                                    if *name == PREDICATE_COLUMN_INDEX.to_string() {
                                        output_schema.num_fields()
                                    } else {
                                        output_schema.index_of(name).unwrap()
                                    }
                                })
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

        let merge_into_build_info = self.merge_into_build_info.clone().unwrap();
        let base_snapshot = merge_into_build_info.table_snapshot;

        let mut field_index_of_input_schema = HashMap::<FieldIndex, usize>::new();
        for (field_index, value) in field_index_map {
            field_index_of_input_schema
                .insert(*field_index, output_schema.index_of(value).unwrap());
        }

        plan = PhysicalPlan::MergeIntoManipulate(Box::new(MergeIntoManipulate {
            plan_id: 0,
            input: Box::new(plan.clone()),
            table_info: table_info.clone(),
            unmatched: unmatched.clone(),
            matched: matched.clone(),
            field_index_of_input_schema: field_index_of_input_schema.clone(),
            merge_type: merge_type.clone(),
            row_id_idx: row_id_offset,
            source_row_id_idx,
            source_row_number_idx,
            enable_right_broadcast: *enable_right_broadcast,
            can_try_update_column_only: *can_try_update_column_only,
            unmatched_schema: join_output_schema.clone(),
        }));

        let merge_into_op = match (merge_type, distributed) {
            (MergeIntoType::FullOperation, true) => MergeIntoOp::DistributedFullOperation,
            (MergeIntoType::FullOperation, false) => MergeIntoOp::StandaloneFullOperation,
            (MergeIntoType::MatchedOnly, true) => MergeIntoOp::DistributedMatchedOnly,
            (MergeIntoType::MatchedOnly, false) => MergeIntoOp::StandaloneMatchedOnly,
            (MergeIntoType::InsertOnly, true) => MergeIntoOp::DistributedInsertOnly,
            (MergeIntoType::InsertOnly, false) => MergeIntoOp::StandaloneInsertOnly,
        };

        plan = PhysicalPlan::MergeIntoOrganize(Box::new(MergeIntoOrganize {
            plan_id: 0,
            input: Box::new(plan.clone()),
            merge_into_op: merge_into_op.clone(),
        }));

        let segments: Vec<_> = base_snapshot
            .segments
            .clone()
            .into_iter()
            .enumerate()
            .collect();

        plan = PhysicalPlan::MergeIntoSerialize(Box::new(MergeIntoSerialize {
            plan_id: 0,
            input: Box::new(plan),
            table_info: table_info.clone(),
            unmatched: unmatched.clone(),
            segments: segments.clone(),
            distributed: *distributed,
            change_join_order: *change_join_order,
            merge_into_op: merge_into_op.clone(),
            need_match: !is_insert_only,
            enable_right_broadcast: *enable_right_broadcast,
        }));

        let commit_input = if !distributed {
            PhysicalPlan::MergeInto(Box::new(MergeInto {
                input: Box::new(plan.clone()),
                table_info: table_info.clone(),
                unmatched,
                distributed: false,
                output_schema: DataSchemaRef::default(),
                merge_into_op: merge_into_op.clone(),
                need_match: !is_insert_only,
                change_join_order: *change_join_order,
                target_build_optimization: false,
                plan_id: u32::MAX,
                enable_right_broadcast: *enable_right_broadcast,
            }))
        } else {
            let merge_append = PhysicalPlan::MergeInto(Box::new(MergeInto {
                input: Box::new(plan.clone()),
                table_info: table_info.clone(),
                unmatched: unmatched.clone(),
                distributed: true,
                output_schema: if let Some(idx) = source_row_number_idx {
                    DataSchemaRef::new(DataSchema::new(vec![output_schema.fields[idx].clone()]))
                } else {
                    DataSchemaRef::new(DataSchema::new(vec![DataField::new(
                        ROW_ID_COL_NAME,
                        DataType::Number(NumberDataType::UInt64),
                    )]))
                },
                merge_into_op: merge_into_op.clone(),
                need_match: !is_insert_only,
                change_join_order: *change_join_order,
                target_build_optimization: false, // we don't support for distributed mode for now.
                plan_id: u32::MAX,
                enable_right_broadcast: *enable_right_broadcast,
            }));
            // if change_join_order = true, it means the target is build side,
            // in this way, we will do matched operation and not matched operation
            // locally in every node, and the main node just receive row ids to apply.
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
                unmatched: unmatched.clone(),
                input_schema: join_output_schema.clone(),
                merge_type: merge_type.clone(),
                change_join_order: *change_join_order,
                segments,
                plan_id: u32::MAX,
            }))
        };

        // build mutation_aggregate
        let mut physical_plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(commit_input),
            snapshot: base_snapshot,
            table_info: table_info.clone(),
            // let's use update first, we will do some optimizations and select exact strategy
            mutation_kind: MutationKind::Update,
            update_stream_meta: merge_into_build_info.update_stream_meta,
            merge_meta: false,
            deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            plan_id: u32::MAX,
        }));
        physical_plan.adjust_plan_id(&mut 0);
        Ok(physical_plan)
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MergeIntoAppendNotMatched {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub table_info: TableInfo,
    // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
    pub unmatched: Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>,
    pub input_schema: DataSchemaRef,
    pub merge_type: MergeIntoType,
    pub change_join_order: bool,
    pub segments: Vec<(usize, Location)>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MergeIntoOp {
    StandaloneMatchedOnly,
    StandaloneFullOperation,
    StandaloneInsertOnly,
    DistributedMatchedOnly,
    DistributedFullOperation,
    DistributedInsertOnly,
}

impl MergeIntoOp {
    pub fn get_serialize_and_row_number_len(
        &self,
        output_len: usize,
        enable_right_broadcast: bool,
    ) -> (usize, usize) {
        match self {
            MergeIntoOp::StandaloneFullOperation
            | MergeIntoOp::StandaloneMatchedOnly
            | MergeIntoOp::DistributedMatchedOnly => (output_len - 1, 0), /* remove first row_id port */
            MergeIntoOp::StandaloneInsertOnly => (output_len, 0),
            MergeIntoOp::DistributedFullOperation => {
                if enable_right_broadcast {
                    // remove first row_id port and last row_number port
                    (output_len - 2, 1)
                } else {
                    // remove first row_id port
                    (output_len - 1, 0)
                }
            }
            MergeIntoOp::DistributedInsertOnly => {
                // only one row_number port/unmatched port, refer to `builder_merge_into_organize`
                assert_eq!(output_len, 1);
                if enable_right_broadcast {
                    // only one row_number port
                    // use (0, 0) instead of (0, 1) to avoid appending many dummy items
                    (0, 0)
                } else {
                    // only one unmatched port
                    (1, 0)
                }
            }
        }
    }
}
