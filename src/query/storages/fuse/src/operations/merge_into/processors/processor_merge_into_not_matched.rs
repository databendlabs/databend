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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SourceSchemaIndex;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_storage::MergeStatus;
use itertools::Itertools;
use log::info;

use crate::operations::merge_into::mutator::SplitByExprMutator;
use crate::operations::BlockMetaIndex;
// (source_schema,condition,values_exprs)
pub type UnMatchedExprs = Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>;

struct InsertDataBlockMutation {
    op: BlockOperator,
    split_mutator: SplitByExprMutator,
}

// need to evaluate expression and

pub struct MergeIntoNotMatchedProcessor {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    ops: Vec<InsertDataBlockMutation>,
    input_data: Option<DataBlock>,
    output_data: Vec<DataBlock>,
    func_ctx: FunctionContext,
    ctx: Arc<dyn TableContext>,
}

impl MergeIntoNotMatchedProcessor {
    pub fn create(
        unmatched: UnMatchedExprs,
        input_schema: DataSchemaRef,
        func_ctx: FunctionContext,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Self> {
        let mut ops = Vec::<InsertDataBlockMutation>::with_capacity(unmatched.len());
        for item in unmatched.iter() {
            let eval_projections: HashSet<usize> =
                (input_schema.num_fields()..input_schema.num_fields() + item.2.len()).collect();
            ops.push(InsertDataBlockMutation {
                op: BlockOperator::Map {
                    exprs: item
                        .2
                        .iter()
                        .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
                        .collect_vec(),
                    projections: Some(eval_projections),
                },
                split_mutator: {
                    let filter = item.1.as_ref().map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS));
                    SplitByExprMutator::create(filter, func_ctx.clone())
                },
            });
        }

        Ok(Self {
            input_port: InputPort::create(),
            output_port: OutputPort::create(),
            ops,
            input_data: None,
            output_data: Vec::new(),
            func_ctx,
            ctx,
        })
    }

    #[allow(dead_code)]
    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port = self.output_port.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output_port])
    }
}

impl Processor for MergeIntoNotMatchedProcessor {
    fn name(&self) -> String {
        "MergeIntoNotMatched".to_owned()
    }

    #[doc = " Reference used for downcast."]
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let finished = self.input_port.is_finished() && self.output_data.is_empty();
        if finished {
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;

        if self.output_port.can_push() && !self.output_data.is_empty() {
            self.output_port
                .push_data(Ok(self.output_data.pop().unwrap()));
            pushed_something = true
        }

        if pushed_something {
            return Ok(Event::NeedConsume);
        }

        if self.input_port.has_data() {
            if self.output_data.is_empty() {
                self.input_data = Some(self.input_port.pull_data().unwrap()?);
                Ok(Event::Sync)
            } else {
                Ok(Event::NeedConsume)
            }
        } else {
            self.input_port.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(mut data_block) = self.input_data.take() {
            if data_block.is_empty() {
                return Ok(());
            }
            // target build optimization, we `take_meta` not `get_meta`, because the `BlockMetaIndex` is
            // just used to judge whether we need to update `merge_status`, we shouldn't pass it through.
            // no_need_add_status means this the origin data block from targe table, and we can push it directly.
            let no_need_add_status = data_block.get_meta().is_some()
                && BlockMetaIndex::downcast_from(data_block.take_meta().unwrap()).is_some();
            if no_need_add_status {
                // no need to give source schema, the data block's schema is complete, so we won'f fill default
                // field values.The computed field will be processed in `TransformResortAddOnWithoutSourceSchema`.
                info!("CHECK: target build optimized, passing data block directly to downstream");
                self.output_data.push(data_block);
                return Ok(());
            }
            let start = Instant::now();
            let mut current_block = data_block;
            for (idx, op) in self.ops.iter().enumerate() {
                let (mut satisfied_block, unsatisfied_block) =
                    op.split_mutator.split_by_expr(current_block)?;
                let source_schema_idx: SourceSchemaIndex = idx;
                satisfied_block = satisfied_block.add_meta(Some(Box::new(source_schema_idx)))?;
                if !satisfied_block.is_empty() {
                    metrics_inc_merge_into_append_blocks_counter(1);
                    metrics_inc_merge_into_append_blocks_rows_counter(
                        satisfied_block.num_rows() as u32
                    );
                    self.ctx.add_merge_status(MergeStatus {
                        insert_rows: satisfied_block.num_rows(),
                        update_rows: 0,
                        deleted_rows: 0,
                    });

                    info!("CHECK: appending data block {:#?} ", satisfied_block);
                    self.output_data
                        .push(op.op.execute(&self.func_ctx, satisfied_block)?)
                }

                if unsatisfied_block.is_empty() {
                    break;
                }
                current_block = unsatisfied_block;
            }
            let elapsed_time = start.elapsed().as_millis() as u64;
            merge_into_not_matched_operation_milliseconds(elapsed_time);
        }

        Ok(())
    }
}
