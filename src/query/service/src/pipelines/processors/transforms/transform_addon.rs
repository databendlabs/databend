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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::ChunkEntry;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::Value;
use common_sql::evaluator::ChunkOperator;
use common_sql::evaluator::CompoundChunkOperator;
use common_sql::parse_exprs;
use common_storages_factory::Table;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;
use crate::sessions::QueryContext;

pub struct TransformAddOn {
    default_nonexpr_fields: Vec<DataField>,

    expression_transform: CompoundChunkOperator,
}

impl TransformAddOn
where Self: Transform
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        input_schema: DataSchemaRef,
        table: Arc<dyn Table>,
        ctx: Arc<QueryContext>,
    ) -> Result<ProcessorPtr> {
        let mut default_exprs = Vec::new();
        let mut default_nonexpr_fields = Vec::new();

        for (index, f) in unresort_fields.iter().enumerate() {
            if !input_schema.has_field(f.name()) {
                if let Some(default_expr) = f.default_expr() {
                    let expr = parse_exprs(ctx.clone(), table.clone(), default_expr)?[0];
                    default_exprs.push(ChunkOperator::Map { index, expr });
                } else {
                    default_nonexpr_fields.push(f.clone());
                }
            }
        }

        let func_ctx = ctx.try_get_function_context()?;
        let expression_transform = CompoundChunkOperator {
            ctx: func_ctx,
            operators: default_exprs,
        };

        Ok(Transformer::create(input, output, Self {
            default_nonexpr_fields,
            expression_transform,
        }))
    }
}

impl Transform for TransformAddOn {
    const NAME: &'static str = "AddOnTransform";

    fn transform(&mut self, mut chunk: Chunk) -> Result<Chunk> {
        let num_rows = chunk.num_rows();
        chunk = self.expression_transform.transform(chunk.clone())?;

        for f in &self.default_nonexpr_fields {
            let default_value = f.data_type().default_value();
            let column = ChunkEntry {
                id: chunk.num_columns(),
                data_type: f.data_type().clone(),
                value: Value::Scalar(default_value),
            };
            chunk.add_column(column);
        }

        Ok(chunk)
    }
}
