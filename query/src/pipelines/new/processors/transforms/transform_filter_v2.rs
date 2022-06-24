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
use common_functions::scalars::FunctionContext;

use crate::common::EvalNode;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::sql::exec::ColumnID;

pub struct TransformFilterV2 {
    predicate: EvalNode<ColumnID>,
    func_ctx: FunctionContext,
}

impl TransformFilterV2 {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        predicate: EvalNode<ColumnID>,
        func_ctx: FunctionContext,
    ) -> Result<ProcessorPtr> {
        Ok(Transformer::create(input, output, Self {
            predicate,
            func_ctx,
        }))
    }
}

impl Transform for TransformFilterV2 {
    const NAME: &'static str = "Filter";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let typed_vector = self.predicate.eval(&self.func_ctx, &data)?;
        let column = typed_vector.vector();
        DataBlock::filter_block(&data, column)
    }
}
