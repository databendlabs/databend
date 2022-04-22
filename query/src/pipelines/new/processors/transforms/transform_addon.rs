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
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;

pub struct TransformAddOn {
    default_expr_fields: Vec<DataField>,
    default_nonexpr_fields: Vec<DataField>,

    expression_executor: ExpressionExecutor,
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
                if let Some(expr) = f.default_expr() {
                    let expression: Expression = serde_json::from_slice::<Expression>(expr)?;
                    let expression = Expression::Alias(
                        f.name().to_string(),
                        Box::new(Expression::Cast {
                            expr: Box::new(expression),
                            data_type: f.data_type().clone(),
                            pg_style: false,
                        }),
                    );

                    default_expr_fields.push(f.clone());
                    default_exprs.push(expression);
                } else {
                    default_nonexpr_fields.push(f.clone());
                }
            }
        }
        let schema_after_default_expr = Arc::new(DataSchema::new(default_expr_fields.clone()));
        let expression_executor = ExpressionExecutor::try_create(
            "stream_addon",
            input_schema,
            schema_after_default_expr,
            default_exprs,
            true,
            ctx,
        )?;

        Ok(Transformer::create(input, output, Self {
            default_expr_fields,
            default_nonexpr_fields,
            expression_executor,
            output_schema,
        }))
    }
}

impl Transform for TransformAddOn {
    const NAME: &'static str = "AddOnTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let expr_block = self.expression_executor.execute(&block)?;

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
