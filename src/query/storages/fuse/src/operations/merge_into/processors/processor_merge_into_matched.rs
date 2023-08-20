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

use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::TableSchemaRef;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

type MatchExpr = Vec<(
    Option<RemoteExpr<String>>,
    Option<Vec<(FieldIndex, RemoteExpr<String>)>>,
)>;

pub struct MergeIntoMatchedProcessor {
    matched: MatchExpr,
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    target_table_schema: TableSchemaRef,
}

impl MergeIntoMatchedProcessor {
    pub fn create(matched: MatchExpr, target_table_schema: TableSchemaRef) -> Result<Self> {
        Ok(Self {
            matched,
            input_port: InputPort::create(),
            output_port: OutputPort::create(),
            target_table_schema,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        todo!()
    }
}

impl Processor for MergeIntoMatchedProcessor {
    fn name(&self) -> String {
        "MergeIntoMatched".to_owned()
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
