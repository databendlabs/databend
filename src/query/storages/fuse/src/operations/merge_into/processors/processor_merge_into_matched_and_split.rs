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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::type_check::check;
use databend_common_expression::types::BooleanType;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::RawExpr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::MatchExpr;
use databend_common_sql::IndexType;
use databend_common_storage::MergeStatus;

use crate::operations::common::MutationLogs;
use crate::operations::merge_into::mutator::DeleteByExprMutator;
use crate::operations::merge_into::mutator::UpdateByExprMutator;
use crate::operations::BlockMetaIndex;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct SourceFullMatched;

#[typetag::serde(name = "source_full_matched")]
impl BlockMetaInfo for SourceFullMatched {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        SourceFullMatched::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[allow(dead_code)]
enum MutationKind {
    Update(UpdateDataBlockMutation),
    Delete(DeleteDataBlockMutation),
}

// if we use hash shuffle join strategy, the enum
// type can't be parser when transform data between nodes.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct MixRowIdKindAndLog {
    pub log: Option<MutationLogs>,
    // kind's range is [0,1,2], 0 stands for log
    // 1 stands for row_id_update, 2 stands for row_id_delete,
    pub kind: usize,
}

#[typetag::serde(name = "mix_row_id_kind_and_log")]
impl BlockMetaInfo for MixRowIdKindAndLog {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        MixRowIdKindAndLog::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum RowIdKind {
    Update,
    Delete,
}

#[typetag::serde(name = "row_id_kind")]
impl BlockMetaInfo for RowIdKind {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        RowIdKind::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

struct UpdateDataBlockMutation {
    update_mutator: UpdateByExprMutator,
}

struct DeleteDataBlockMutation {
    delete_mutator: DeleteByExprMutator,
}

pub struct MatchedSplitProcessor {
    input_port: Arc<InputPort>,
    output_port_row_id: Arc<OutputPort>,
    output_port_updated: Arc<OutputPort>,
    ops: Vec<MutationKind>,
    ctx: Arc<dyn TableContext>,
    update_projections: Vec<usize>,
    row_id_idx: usize,
    input_data: Option<DataBlock>,
    output_data_row_id_data: Vec<DataBlock>,
    output_data_updated_data: Option<DataBlock>,
    target_table_schema: DataSchemaRef,
    target_build_optimization: bool,
    enable_update_column_only: bool,
}

impl MatchedSplitProcessor {
    pub fn try_update_column_only(
        update_projections: &mut Vec<usize>,
        matched: MatchExpr,
        field_index_of_input_schema: HashMap<FieldIndex, usize>,
    ) -> bool {
        assert_eq!(matched.len(), 1);
        let item = &matched[0];
        // there is no condition
        assert!(item.0.is_none());
        // it's update not delete.
        assert!(item.1.is_some());
        let update_exprs = item.1.as_ref().unwrap();
        let mut update_field_indexes: HashMap<FieldIndex, IndexType> =
            HashMap::with_capacity(update_exprs.len());
        for item in update_exprs.iter() {
            let mut update_set_expr = if let RemoteExpr::FunctionCall { id, args, .. } = &item.1 {
                assert_eq!(id.name(), "if");
                // the predcate is always true.
                &args[1]
            } else {
                unreachable!()
            };
            // in `generate_update_list` we will do `wrap_cast_expr` to cast `left` into dest_type,
            // but after that, we will do a `type_check` in `scalar.as_expr`, if the cast's dest_type
            // is the same, we will deref the `cast`.
            if let RemoteExpr::Cast { expr, .. } = update_set_expr {
                update_set_expr = expr.as_ref();
            }

            if let RemoteExpr::ColumnRef { id, .. } = update_set_expr {
                // (field_index,project_idx)
                update_field_indexes.insert(item.0, *id);
            } else {
                return false;
            }
        }

        // `field_index_of_input_schema` contains all columns of target_table
        for field_index in 0..field_index_of_input_schema.len() {
            if update_field_indexes.contains_key(&field_index) {
                update_projections.push(*update_field_indexes.get(&field_index).unwrap());
            } else {
                update_projections.push(*field_index_of_input_schema.get(&field_index).unwrap());
            }
        }
        true
    }

    pub fn create(
        ctx: Arc<dyn TableContext>,
        row_id_idx: usize,
        matched: MatchExpr,
        field_index_of_input_schema: HashMap<FieldIndex, usize>,
        input_schema: DataSchemaRef,
        target_table_schema: DataSchemaRef,
        target_build_optimization: bool,
        can_try_update_column_only: bool,
    ) -> Result<Self> {
        let mut update_projections = Vec::with_capacity(field_index_of_input_schema.len());
        let mut ops = Vec::<MutationKind>::new();
        let mut enable_update_column_only = false;

        if can_try_update_column_only {
            enable_update_column_only = MatchedSplitProcessor::try_update_column_only(
                &mut update_projections,
                matched.clone(),
                field_index_of_input_schema.clone(),
            );
        }

        if !enable_update_column_only {
            for item in matched.iter() {
                // delete
                if item.1.is_none() {
                    let filter = item.0.as_ref().map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS));
                    ops.push(MutationKind::Delete(DeleteDataBlockMutation {
                        delete_mutator: DeleteByExprMutator::create(
                            filter.clone(),
                            ctx.get_function_context()?,
                            row_id_idx,
                            input_schema.num_fields(),
                            target_build_optimization,
                        ),
                    }))
                } else {
                    let update_lists = item.1.as_ref().unwrap();
                    let filter = item
                        .0
                        .as_ref()
                        .map(|condition| condition.as_expr(&BUILTIN_FUNCTIONS));

                    ops.push(MutationKind::Update(UpdateDataBlockMutation {
                        update_mutator: UpdateByExprMutator::create(
                            filter,
                            ctx.get_function_context()?,
                            field_index_of_input_schema.clone(),
                            update_lists.clone(),
                            input_schema.num_fields(),
                        ),
                    }))
                }
            }

            // `field_index_of_input_schema` contains all columns of target_table
            for field_index in 0..field_index_of_input_schema.len() {
                update_projections.push(*field_index_of_input_schema.get(&field_index).unwrap());
            }
        }

        let input_port = InputPort::create();
        let output_port_row_id = OutputPort::create();
        let output_port_updated = OutputPort::create();
        Ok(Self {
            ctx,
            input_port,
            output_data_row_id_data: Vec::new(),
            output_data_updated_data: None,
            input_data: None,
            output_port_row_id,
            output_port_updated,
            ops,
            row_id_idx,
            update_projections,
            target_table_schema,
            target_build_optimization,
            enable_update_column_only,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port_row_id = self.output_port_row_id.clone();
        let output_port_updated = self.output_port_updated.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![
            output_port_row_id,
            output_port_updated,
        ])
    }
}

impl Processor for MatchedSplitProcessor {
    fn name(&self) -> String {
        "MatchedSplit".to_owned()
    }

    #[doc = " Reference used for downcast."]
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // 1. if there is no data and input_port is finished, this processor has finished
        // it's work
        let finished = self.input_port.is_finished()
            && self.output_data_row_id_data.is_empty()
            && self.output_data_updated_data.is_none();
        if finished {
            self.output_port_row_id.finish();
            self.output_port_updated.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;

        // 2. process data stage here
        if self.output_port_row_id.can_push() && !self.output_data_row_id_data.is_empty() {
            self.output_port_row_id
                .push_data(Ok(self.output_data_row_id_data.pop().unwrap()));
            pushed_something = true
        }

        if self.output_port_updated.can_push() {
            if let Some(update_data) = self.output_data_updated_data.take() {
                self.output_port_updated.push_data(Ok(update_data));
                pushed_something = true
            }
        }

        // 3. trigger down stream pipeItem to consume if we pushed data
        if pushed_something {
            Ok(Event::NeedConsume)
        } else {
            // 4. we can't pushed data ,so the down stream is not prepared or we have no data at all
            // we need to make sure only when the all out_pudt_data are empty ,and we start to split
            // datablock held by input_data
            if self.input_port.has_data() {
                if self.output_data_row_id_data.is_empty()
                    && self.output_data_updated_data.is_none()
                {
                    // no pending data (being sent to down streams)
                    self.input_data = Some(self.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                } else {
                    // data pending
                    Ok(Event::NeedConsume)
                }
            } else {
                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            //  we receive a partial unmodified block data meta.
            if data_block.get_meta().is_some() && data_block.is_empty() {
                assert!(self.target_build_optimization);
                let meta_index = BlockMetaIndex::downcast_ref_from(data_block.get_meta().unwrap());
                if meta_index.is_some() {
                    self.output_data_row_id_data.push(data_block);
                    return Ok(());
                }
            }

            if data_block.is_empty() {
                return Ok(());
            }
            // insert-only, we need to remove this pipeline according to strategy.
            if self.ops.is_empty() && !self.enable_update_column_only {
                return Ok(());
            }
            let start = Instant::now();
            let mut current_block = data_block;
            if !self.enable_update_column_only {
                for op in self.ops.iter() {
                    match op {
                        MutationKind::Update(update_mutation) => {
                            let stage_block = update_mutation
                                .update_mutator
                                .update_by_expr(current_block)?;
                            current_block = stage_block;
                        }

                        MutationKind::Delete(delete_mutation) => {
                            let (stage_block, mut row_ids) = delete_mutation
                                .delete_mutator
                                .delete_by_expr(current_block)?;

                            // delete all
                            if !row_ids.is_empty() {
                                row_ids = row_ids.add_meta(Some(Box::new(RowIdKind::Delete)))?;
                                self.output_data_row_id_data.push(row_ids);
                            }

                            if stage_block.is_empty() {
                                return Ok(());
                            }
                            current_block = stage_block;
                        }
                    }
                }

                let filter: Value<BooleanType> = current_block
                    .get_by_offset(current_block.num_columns() - 1)
                    .value
                    .try_downcast()
                    .unwrap();
                current_block = current_block.filter_boolean_value(&filter)?;
            }

            if !current_block.is_empty() {
                // add updated row_ids
                self.ctx.add_merge_status(MergeStatus {
                    insert_rows: 0,
                    update_rows: current_block.num_rows(),
                    deleted_rows: 0,
                });

                // for target build optimization, there is only one matched clause without condition. we won't read rowid.
                if !self.target_build_optimization {
                    self.output_data_row_id_data.push(DataBlock::new_with_meta(
                        vec![current_block.get_by_offset(self.row_id_idx).clone()],
                        current_block.num_rows(),
                        Some(Box::new(RowIdKind::Update)),
                    ));
                }

                let op = BlockOperator::Project {
                    projection: self.update_projections.clone(),
                };
                current_block = op.execute(&self.ctx.get_function_context()?, current_block)?;
                metrics_inc_merge_into_append_blocks_counter(1);
                metrics_inc_merge_into_append_blocks_rows_counter(current_block.num_rows() as u32);

                current_block = self.cast_data_type_for_merge(current_block)?;

                // no need to give source schema, the data block's schema is complete, so we won'f fill default
                // field values.The computed field will be processed in `TransformResortAddOnWithoutSourceSchema`.
                self.output_data_updated_data = Some(current_block);
            }
            let elapsed_time = start.elapsed().as_millis() as u64;
            merge_into_matched_operation_milliseconds(elapsed_time);
        }
        Ok(())
    }
}

impl MatchedSplitProcessor {
    fn cast_data_type_for_merge(&self, current_block: DataBlock) -> Result<DataBlock> {
        // cornor case: for merge into update, if the target table's column is not null,
        // for example, target table has three columns like (a,b,c), and we use update set target_table.a = xxx,
        // it's fine because we have cast the xxx'data_type into a's data_type in `generate_update_list()`,
        // but for b,c, the hash table will transform the origin data_type (b_type,c_type) into
        // (nullable(b_type),nullable(c_type)), so we will get datatype not match error, let's transform
        // them back here.
        let current_columns = current_block.columns();
        assert_eq!(
            self.target_table_schema.fields.len(),
            current_columns.len(),
            "target table columns and current columns length mismatch"
        );
        let cast_exprs = current_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                check(
                    &RawExpr::Cast {
                        span: None,
                        is_try: false,
                        expr: Box::new(RawExpr::ColumnRef {
                            span: None,
                            id: idx,
                            data_type: col.data_type.clone(),
                            display_name: "".to_string(),
                        }),
                        dest_type: self.target_table_schema.fields[idx].data_type().clone(),
                    },
                    &BUILTIN_FUNCTIONS,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let cast_operator = BlockOperator::Map {
            exprs: cast_exprs,
            projections: Some((current_columns.len()..current_columns.len() * 2).collect()),
        };
        cast_operator.execute(&self.ctx.get_function_context()?, current_block)
    }
}
