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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;

use crate::processors::transforms::Transform;
use crate::processors::transforms::Transformer;

pub struct TransformDummy;

impl TransformDummy {
    #[allow(dead_code)]
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
        ProcessorPtr::create(Transformer::create(input, output, TransformDummy {}))
    }
}

#[async_trait::async_trait]
impl Transform for TransformDummy {
    const NAME: &'static str = "DummyTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        Ok(data)
    }
}

pub fn create_dummy_item() -> PipeItem {
    let input = InputPort::create();
    let output = OutputPort::create();
    PipeItem::create(
        TransformDummy::create(input.clone(), output.clone()),
        vec![input],
        vec![output],
    )
}

pub fn create_dummy_items(size: usize, capacity: usize) -> Vec<PipeItem> {
    let mut items = Vec::with_capacity(capacity);

    for _index in 0..size {
        items.push(create_dummy_item());
    }

    items
}
