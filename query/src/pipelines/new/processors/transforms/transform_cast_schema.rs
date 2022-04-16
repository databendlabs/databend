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

use common_datablocks::DataBlock;
use common_datavalues::ColumnWithField;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionContext;
use common_streams::SendableDataBlockStream;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::pipelines::new::processors::ResizeProcessor;

pub struct TransformCastSchema {
    output_schema: DataSchemaRef,
    functions: Vec<Box<dyn Function>>,
    func_ctx: FunctionContext,
}

impl TransformCastSchema
where Self: Transform
{
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        functions: Vec<Box<dyn Function>>,
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

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let rows = data.num_rows();
        let iter = self
            .functions
            .iter()
            .zip(data.schema().fields())
            .zip(data.columns());
        let mut columns = Vec::with_capacity(data.num_columns());
        for ((cast_func, input_field), column) in iter {
            let column = ColumnWithField::new(column.clone(), input_field.clone());
            columns.push(cast_func.eval(self.func_ctx.clone(), &[column], rows)?);
        }
        Ok(DataBlock::create(self.output_schema.clone(), columns))
    }
}
