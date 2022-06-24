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
use common_datavalues::DataField;
use common_exception::Result;
use common_functions::scalars::FunctionContext;

use crate::common::EvalNode;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::sql::exec::ColumnID;

pub struct ExpressionTransformV2 {
    expressions: Vec<(EvalNode<ColumnID>, String)>,
    func_ctx: FunctionContext,
}

impl ExpressionTransformV2 {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        expressions: Vec<(EvalNode<ColumnID>, String)>,
        func_ctx: FunctionContext,
    ) -> ProcessorPtr {
        Transformer::create(input, output, Self {
            expressions,
            func_ctx,
        })
    }
}

impl Transform for ExpressionTransformV2 {
    const NAME: &'static str = "Expression";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        for (eval, output_name) in self.expressions.iter() {
            let typed_vector = eval.eval(&self.func_ctx, &data)?;
            let data_field = DataField::new(output_name.as_str(), typed_vector.logical_type());
            let column = typed_vector.vector().clone();
            data = data.add_column(column, data_field)?;
        }

        Ok(data)
    }
}
