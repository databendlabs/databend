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
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;

pub struct TransformProject {
    projections: Vec<usize>,
}

impl TransformProject {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        projections: Vec<usize>,
    ) -> ProcessorPtr {
        Transformer::create(input, output, Self { projections })
    }
}

impl Transform for TransformProject {
    const NAME: &'static str = "Project";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let mut result = DataBlock::empty();
        for i in self.projections.iter() {
            result = result.add_column(data.column(*i).clone(), data.schema().field(*i).clone())?;
        }

        Ok(result)
    }
}
