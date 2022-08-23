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
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_exception::Result;

use crate::evaluator::Evaluator;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;
use crate::pipelines::processors::transforms::ExpressionTransformV2;
use crate::sessions::QueryContext;

pub struct TransformAddOn {
    default_expr_fields: Vec<DataField>,
    default_nonexpr_fields: Vec<DataField>,

    expression_transform: ExpressionTransformV2,
    output_schema: DataSchemaRef,
}

impl TransformAddOn
where Self: Transform
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        ctx: Arc<QueryContext>,
    ) -> Result<ProcessorPtr> {
        let mut default_expr_fields = Vec::new();
        let mut default_exprs = Vec::new();
        let mut default_nonexpr_fields = Vec::new();

        for f in output_schema.fields() {
            if !input_schema.has_field(f.name()) {
                if let Some(default_expr) = f.default_expr() {
                    default_exprs.push((
                        Evaluator::eval_physical_scalar(&serde_json::from_str(default_expr)?)?,
                        f.name().to_string(),
                    ));
                    default_expr_fields.push(f.clone());
                } else {
                    default_nonexpr_fields.push(f.clone());
                }
            }
        }

        let func_ctx = ctx.try_get_function_context()?;
        let expression_transform = ExpressionTransformV2 {
            expressions: default_exprs,
            func_ctx,
        };

        Ok(Transformer::create(input, output, Self {
            default_expr_fields,
            default_nonexpr_fields,
            expression_transform,
            output_schema,
        }))
    }
}

impl Transform for TransformAddOn {
    const NAME: &'static str = "AddOnTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let expr_block = self.expression_transform.transform(block.clone())?;

        for f in self.default_expr_fields.iter() {
            block =
                block.add_column(expr_block.try_column_by_name(f.name())?.clone(), f.clone())?;
        }

        for f in &self.default_nonexpr_fields {
            let default_value = f.data_type().default_value();
            let column = f
                .data_type()
                .create_constant_column(&default_value, num_rows)?;

            block = block.add_column(column, f.clone())?;
        }
        block.resort(self.output_schema.clone())
    }
}
