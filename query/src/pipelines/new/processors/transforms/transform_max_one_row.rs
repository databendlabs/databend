// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;

pub struct TransformMax1Row {
    finished: bool,
}

impl TransformMax1Row {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
        Transformer::<Self>::create(input, output, Self { finished: false })
    }
}

impl Transform for TransformMax1Row {
    const NAME: &'static str = "Max1Row";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if !self.finished && data.num_rows() == 1 {
            self.finished = true;
            Ok(data)
        } else {
            Err(ErrorCode::LogicalError(
                "more than one row returned by a subquery used as an expression",
            ))
        }
    }
}
