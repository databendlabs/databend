// Copyright 2021 Datafuse Labs.
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
use common_expression::Chunk;
use common_expression::DataSchemaRef;
use common_expression::Function;
use common_expression::FunctionContext;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;

pub struct TransformCastSchema {
    output_schema: DataSchemaRef,
    functions: Vec<Function>,
    func_ctx: FunctionContext,
}

impl TransformCastSchema
where Self: Transform
{
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        functions: Vec<Function>,
        func_ctx: FunctionContext,
    ) -> Result<ProcessorPtr> {
        Ok(Transformer::create(input_port, output_port, Self {
            output_schema,
            functions,
            func_ctx,
        }))
    }
}

impl Transform for TransformCastSchema {
    const NAME: &'static str = "CastSchemaTransform";

    fn transform(&mut self, data: Chunk) -> Result<Chunk> {
        let rows = data.num_rows();
        let mut columns = Vec::with_capacity(data.num_columns());
        for (cast_func, (value, ty)) in self.functions.iter().zip(data.columns()) {
            let v = (cast_func.eval)(&[value.as_ref()], &self.func_ctx)?;
            columns.push((v, ty.clone()));
        }
        Ok(Chunk::new(columns, rows))
    }
}
