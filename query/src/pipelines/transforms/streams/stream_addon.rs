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
use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use futures::Stream;
use futures::StreamExt;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;

/// Add missing column into the block stream
pub struct AddOnStream {
    input: SendableDataBlockStream,

    default_expr_fields: Vec<DataField>,
    default_nonexpr_fields: Vec<DataField>,

    expression_executor: ExpressionExecutor,
    sort_columns_descriptions: Vec<SortColumnDescription>,
    output_schema: DataSchemaRef,
}

impl AddOnStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        cluster_keys: Vec<Expression>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        ctx: Arc<QueryContext>,
    ) -> Result<Self> {
        let mut default_expr_fields = Vec::new();
        let mut default_exprs = Vec::new();
        let mut default_nonexpr_fields = Vec::new();

        for f in output_schema.fields() {
            if !input_schema.has_field(f.name()) {
                if let Some(expr) = f.default_expr() {
                    let expression = PlanParser::parse_expr(expr)?;
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

        let mut output_schema = output_schema;
        let mut sort_columns_descriptions = Vec::new();
        if !cluster_keys.is_empty() {
            let mut merged = output_schema.fields().clone();
            for expr in &cluster_keys {
                let cname = expr.column_name();
                if !merged.iter().any(|x| x.name() == &cname) {
                    let field = expr.to_data_field(&input_schema)?;
                    merged.push(field.clone());

                    default_expr_fields.push(field);
                    default_exprs.push(expr.clone());
                }
            }

            output_schema = DataSchemaRefExt::create(merged);

            // sort
            sort_columns_descriptions = cluster_keys
                .iter()
                .map(|expr| SortColumnDescription {
                    column_name: expr.column_name(),
                    asc: true,
                    nulls_first: false,
                })
                .collect();
        }

        let schema_after_default_expr = DataSchemaRefExt::create(default_expr_fields.clone());
        let expression_executor = ExpressionExecutor::try_create(
            ctx,
            "stream_addon",
            input_schema,
            schema_after_default_expr,
            default_exprs,
            true,
        )?;

        Ok(AddOnStream {
            input,
            default_expr_fields,
            default_nonexpr_fields,
            expression_executor,
            sort_columns_descriptions,
            output_schema,
        })
    }

    #[inline]
    fn add_missing_column(&self, mut block: DataBlock) -> Result<DataBlock> {
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

        let block = block.resort(self.output_schema.clone())?;
        if self.sort_columns_descriptions.is_empty() {
            Ok(block)
        } else {
            DataBlock::sort_block(&block, &self.sort_columns_descriptions, None)
        }
    }
}

impl Stream for AddOnStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(self.add_missing_column(v)),
            other => other,
        })
    }
}
