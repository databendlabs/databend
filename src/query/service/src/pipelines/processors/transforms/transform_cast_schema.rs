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
use common_expression::ChunkEntry;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Function;
use common_expression::FunctionContext;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;

pub struct TransformCastSchema {
    func_ctx: FunctionContext,
    exprs: Vec<Expr>,
}

impl TransformCastSchema
where Self: Transform
{
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        select_schema: DataSchemaRef,
        insert_schema: DataSchemaRef,
        func_ctx: FunctionContext,
    ) -> Result<ProcessorPtr> {
        let exprs = select_schema
            .fields()
            .iter()
            .zip(insert_schema.fields().iter().enumerate())
            .map(|(from, (index, to))| {
                let expr = Expr::ColumnRef {
                    span: None,
                    id: index,
                    data_type: from.data_type().clone(),
                };
                Expr::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(expr),
                    dest_type: to.data_type().clone(),
                }
            })
            .collect();
        Ok(Transformer::create(input_port, output_port, Self {
            func_ctx,
            exprs,
        }))
    }
}

impl Transform for TransformCastSchema {
    const NAME: &'static str = "CastSchemaTransform";

    fn transform(&mut self, data: Chunk) -> Result<Chunk> {
        todo!("expression")
        // let rows = data.num_rows();

        // let evaluator = Evaluator::new(&data, self.func_ctx.clone(), &BUILTIN_FUNCTIONS);

        // let indices = evaluator.run(&self.indices_scalar)?;
        // let indices = get_hash_values(&indices, num)?;
        // let chunks = Chunk::scatter(chunk, &indices, self.scatter_size)?;

        // let mut chunk = Chunk::new(vec![], rows);

        // for (index, f) in self.output_schema.fields().iter().enumerate() {
        //     let col = data.get_by_offset(index);
        //     f.data_type();
        // }

        // let mut columns = Vec::with_capacity(data.num_columns());
        // for (cast_func, entry) in self.functions.iter().zip(data.columns()) {
        //     let v = (cast_func.eval)(&[value.as_ref()], &self.func_ctx)?;
        //     columns.push((v, ty.clone()));
        // }
        // Ok(Chunk::new(columns, rows))
    }
}
