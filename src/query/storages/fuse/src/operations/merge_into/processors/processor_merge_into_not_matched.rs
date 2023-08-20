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
use std::sync::Arc;

use ahash::HashMap;
use ahash::HashMapExt;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::RemoteExpr;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

use crate::operations::mutation::BlockIndex;

type UnMatchedExprs = Vec<(
    DataSchemaRef,
    Option<RemoteExpr>,
    Vec<RemoteExpr>,
    Vec<usize>,
)>;
// need to evaluate expression and
pub struct MergeIntoNotMatchedProcessor {
    unmatched: Vec<(
        DataSchemaRef,
        Option<RemoteExpr>,
        Vec<RemoteExpr>,
        Vec<usize>,
    )>,
    // block_mutator, store new data after update,
    // BlockIndex => (unmatched_expr_idx,(new_data,remain_columns))
    updatede_block: HashMap<BlockIndex, HashMap<u32, (DataBlock, Vec<u32>)>>,
    // store the row_id which is deleted/updated
    block_mutation_row_offset: HashMap<BlockIndex, u32>,
    row_id_idx: u32,
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
}

impl MergeIntoNotMatchedProcessor {
    pub fn create(row_id_idx: u32, unmatched: UnMatchedExprs) -> Result<Self> {
        Ok(Self {
            unmatched,
            row_id_idx,
            updatede_block: HashMap::new(),
            block_mutation_row_offset: HashMap::new(),
            input_port: InputPort::create(),
            output_port: OutputPort::create(),
        })
    }

    pub fn into_pipe_item(&self) -> PipeItem {
        todo!()
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
        todo!()
    }

    fn process(&mut self) -> Result<()> {
        todo!()
    }
}
