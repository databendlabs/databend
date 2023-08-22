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
use std::collections::HashSet;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::BlockOperator;

use crate::operations::mutation::BlockIndex;
type MatchExpr = Vec<(Option<RemoteExpr>, Option<Vec<(FieldIndex, RemoteExpr)>>)>;

enum MutationKind {
    Update(UpdateDataBlockMutation),
    Delete(DeleteDataBlockMutation),
}

enum State {
    RecevingData,
    FinishedOutPut,
}

impl State {
    fn is_finished(&self) -> bool {
        if let State::FinishedOutPut = self {
            true
        } else {
            false
        }
    }
}

struct UpdateDataBlockMutation {
    op: BlockOperator,
    filter: Option<Expr>,
}

struct DeleteDataBlockMutation {
    filter: Option<Expr>,
}

pub struct MergeIntoMatchedProcessor {
    input_port: Arc<InputPort>,
    // let make it parallel in the next pr,for now, just a
    // simple version
    output_port: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    // used to read remain columns
    target_table_schema: TableSchemaRef,
    // (update_idx,remain_columns)
    remain_projections_map: HashMap<usize, Vec<usize>>,
    // block_mutator, store new data after update,
    // BlockIndex => (update_idx,new_data)
    updatede_block: HashMap<BlockIndex, HashMap<usize, DataBlock>>,
    // store the row_id which is deleted/updated
    block_mutation_row_offset: HashMap<BlockIndex, u32>,
    row_id_idx: u32,
    ops: Vec<MutationKind>,
    state: State,
}

impl MergeIntoMatchedProcessor {
    pub fn create(
        row_id_idx: u32,
        matched: MatchExpr,
        target_table_schema: TableSchemaRef,
        input_schema: DataSchemaRef,
    ) -> Result<Self> {
        let mut ops = Vec::<MutationKind>::new();
        let mut remain_projections_map = HashMap::new();
        for (expr_idx, item) in matched.iter().enumerate() {
            // delete
            if item.1.is_none() {
                ops.push(MutationKind::Delete(DeleteDataBlockMutation {
                    filter: match &item.0 {
                        None => None,
                        Some(expr) => Some(expr.as_expr(&BUILTIN_FUNCTIONS)),
                    },
                }))
            } else {
                let update_lists = item.1.as_ref().unwrap();
                let mut set = HashSet::new();
                let mut remain_projections = Vec::new();
                let input_len = input_schema.num_fields();
                let eval_projections: HashSet<usize> =
                    (input_len..update_lists.len() + input_len).collect();

                for (idx, _) in update_lists {
                    set.insert(idx);
                }
                for idx in 0..target_table_schema.num_fields() {
                    if !set.contains(&idx) {
                        remain_projections.push(idx);
                    }
                }

                let exprs: Vec<Expr> = update_lists
                    .iter()
                    .map(|item| item.1.as_expr(&BUILTIN_FUNCTIONS))
                    .collect();

                remain_projections_map.insert(expr_idx, remain_projections);
                ops.push(MutationKind::Update(UpdateDataBlockMutation {
                    op: BlockOperator::Map {
                        exprs,
                        projections: Some(eval_projections),
                    },
                    filter: match &item.0 {
                        None => None,
                        Some(condition) => Some(condition.as_expr(&BUILTIN_FUNCTIONS)),
                    },
                }))
            }
        }

        Ok(Self {
            input_port: InputPort::create(),
            output_port: OutputPort::create(),
            target_table_schema,
            updatede_block: HashMap::new(),
            block_mutation_row_offset: HashMap::new(),
            row_id_idx,
            remain_projections_map,
            ops,
            input_data: None,
            state: State::RecevingData,
            output_data: None,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port = self.output_port.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output_port])
    }
}

#[async_trait::async_trait]
impl Processor for MergeIntoMatchedProcessor {
    fn name(&self) -> String {
        "MergeIntoMatched".to_owned()
    }

    #[doc = " Reference used for downcast."]
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // if no_data coming and we have sent out all processed data to down-stream, it's over.
        let finished = self.input_port.is_finished() && self.state.is_finished();

        if finished {
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        // if have data, we will evalute it
        if !self.input_port.is_finished() {
            if self.input_port.has_data() {
                self.input_data = Some(self.input_port.pull_data().unwrap()?);
                return Ok(Event::Sync);
            } else {
                return Ok(Event::NeedData);
            }
        } else {
            let mut pushed_something = false;

            if self.output_port.can_push() {
                if let Some(matched_data) = self.output_data.take() {
                    self.output_port.push_data(Ok(matched_data));
                    pushed_something = true
                }
            }

            if pushed_something {
                return Ok(Event::NeedConsume);
            }

            return Ok(Event::Async);
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            if data_block.is_empty() {
                return Ok(());
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        Ok(())
    }
}
