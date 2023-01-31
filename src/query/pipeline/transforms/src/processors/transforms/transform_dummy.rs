// Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;

use crate::processors::transforms::Transform;
use crate::processors::transforms::Transformer;

pub struct TransformDummy;

impl TransformDummy {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
        Transformer::create(input, output, TransformDummy)
    }
}

impl Transform for TransformDummy {
    const NAME: &'static str = "TransformDummy";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        Ok(data)
    }
}
