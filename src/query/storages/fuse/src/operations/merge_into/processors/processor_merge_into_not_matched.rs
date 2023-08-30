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

use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::BlockOperator;
use common_storage::common_metrics::merge_into::metrics_inc_merge_into_append_blocks_counter;
use itertools::Itertools;

use crate::operations::merge_into::mutator::SplitByExprMutator;

type UnMatchedExprs = Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>;

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
    output_data: Option<DataBlock>,
    ctx: Arc<dyn TableContext>,
}

impl MergeIntoNotMatchedProcessor {
    pub fn create(
        unmatched: UnMatchedExprs,
        input_schema: DataSchemaRef,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Self> {
        let mut ops = Vec::<InsertDataBlockMutation>::with_capacity(unmatched.len());
        for item in &unmatched {
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
                    SplitByExprMutator::create(filter, ctx.get_function_context()?)
                },
            });
        }

        Ok(Self {
            input_port: InputPort::create(),
            output_port: OutputPort::create(),
            ops,
            input_data: None,
            output_data: None,
            ctx,
        })
    }

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
        let finished = self.input_port.is_finished() && self.output_data.is_none();
        if finished {
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;

        if self.output_port.can_push() {
            if let Some(not_matched_data) = self.output_data.take() {
                self.output_port.push_data(Ok(not_matched_data));
                pushed_something = true
            }
        }

        if pushed_something {
            return Ok(Event::NeedConsume);
        }

        if self.input_port.has_data() {
            if self.output_data.is_none() {
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
        if let Some(data_block) = self.input_data.take() {
            if data_block.is_empty() {
                return Ok(());
            }
            // get an empty data_block but have same schema
            let mut output_block = None;
            let mut current_block = data_block;
            for op in &self.ops {
                let (satisfied_block, unsatisfied_block) =
                    op.split_mutator.split_by_expr(current_block)?;
                // in V1, we make sure the output_schema of each insert expr result block is the same
                // we will fix it in the future.
                if !satisfied_block.is_empty() {
                    if output_block.is_some() {
                        output_block = Some(DataBlock::concat(&[
                            output_block.unwrap(),
                            op.op
                                .execute(&self.ctx.get_function_context()?, satisfied_block)?,
                        ])?);
                    } else {
                        output_block = Some(
                            op.op
                                .execute(&self.ctx.get_function_context()?, satisfied_block)?,
                        )
                    }
                }

                if unsatisfied_block.is_empty() {
                    break;
                } else {
                    current_block = unsatisfied_block
                }
            }

            // todo:(JackTan25) fill format data block
            if output_block.is_some() {
                let block = output_block.as_ref().unwrap();
                let affetcted_rows = block.num_rows();
                metrics_inc_merge_into_append_blocks_counter(affetcted_rows as u32);
                let progress_values = ProgressValues {
                    rows: affetcted_rows,
                    bytes: block.memory_size(),
                };
                self.ctx.get_write_progress().incr(&progress_values);
                self.output_data = output_block
            }
        }
        Ok(())
    }
}
