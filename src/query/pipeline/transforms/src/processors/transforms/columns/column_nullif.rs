// Copyright 2021 Datafuse Labs
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

use databend_common_ast::Span;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnIndex;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;

use crate::processors::Transform;
use crate::processors::Transformer;

pub struct TransformNullIf {
    func_ctx: Arc<FunctionContext>,
    schema: DataSchemaRef,
    exprs: Vec<Expr>,
}

impl TransformNullIf
where Self: Transform
{
    pub fn try_new(
        select_schema: DataSchemaRef,
        insert_schema: DataSchemaRef,
        func_ctx: Arc<FunctionContext>,
        null_str_list: &[String],
    ) -> Result<Self> {
        let exprs = select_schema
            .fields()
            .iter()
            .zip(insert_schema.fields().iter().enumerate())
            .map(|(from, (index, to))| {
                let expr = ColumnRef {
                    span: None,
                    id: index,
                    data_type: from.data_type().clone(),
                    display_name: from.name().clone(),
                };
                Self::try_null_if(
                    None,
                    expr.into(),
                    to.data_type(),
                    &BUILTIN_FUNCTIONS,
                    null_str_list,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Self::new_schema(&select_schema);
        Ok(Self {
            func_ctx,
            schema,
            exprs,
        })
    }
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        select_schema: DataSchemaRef,
        insert_schema: DataSchemaRef,
        func_ctx: Arc<FunctionContext>,
        null_str_list: &[String],
    ) -> Result<ProcessorPtr> {
        let exprs = select_schema
            .fields()
            .iter()
            .zip(insert_schema.fields().iter().enumerate())
            .map(|(from, (index, to))| {
                let expr = ColumnRef {
                    span: None,
                    id: index,
                    data_type: from.data_type().clone(),
                    display_name: from.name().clone(),
                };
                Self::try_null_if(
                    None,
                    expr.into(),
                    to.data_type(),
                    &BUILTIN_FUNCTIONS,
                    null_str_list,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Self::new_schema(&select_schema);
        Ok(ProcessorPtr::create(Transformer::create(
            input_port,
            output_port,
            Self {
                func_ctx,
                schema,
                exprs,
            },
        )))
    }

    pub fn column_need_transform(src_type: &DataType, dest_type: &DataType) -> bool {
        dest_type.is_nullable()
            && match src_type {
                DataType::String => true,
                DataType::Nullable(b) if matches!(**b, DataType::String) => true,
                _ => false,
            }
    }

    pub fn new_schema(schema: &Arc<DataSchema>) -> Arc<DataSchema> {
        let mut schema = schema.as_ref().clone();
        for field in &mut schema.fields {
            if let DataType::String = field.data_type() {
                *field =
                    DataField::new(field.name(), DataType::Nullable(Box::new(DataType::String)))
            }
        }
        Arc::new(schema)
    }

    fn try_null_if<Index: ColumnIndex>(
        span: Span,
        expr: Expr<Index>,
        dest_type: &DataType,
        fn_registry: &FunctionRegistry,
        null_str_list: &[String],
    ) -> Result<Expr<Index>> {
        let src_type = expr.data_type();
        let column = StringType::from_data(null_str_list.to_vec());
        if !Self::column_need_transform(src_type, dest_type) {
            return Ok(expr);
        }
        let args = [
            Constant {
                span,
                scalar: Scalar::Array(column),
                data_type: DataType::Array(Box::new(DataType::String)),
            }
            .into(),
            expr.clone(),
        ];
        check_function(
            span,
            "if",
            &[],
            &[
                check_function(span, "contains", &[], &args, fn_registry)?,
                Constant {
                    span,
                    scalar: Scalar::Null,
                    data_type: DataType::String.wrap_nullable(),
                }
                .into(),
                expr,
            ],
            fn_registry,
        )
    }
}

impl Transform for TransformNullIf {
    const NAME: &'static str = "NullIfTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let mut entries = Vec::with_capacity(self.exprs.len());
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num_rows = data_block.num_rows();
        for (field, expr) in self.schema.fields().iter().zip(self.exprs.iter()) {
            let entry = BlockEntry::new(evaluator.run(expr)?, || {
                (field.data_type().clone(), num_rows)
            });
            entries.push(entry);
        }
        Ok(DataBlock::new(entries, data_block.num_rows()))
    }
}
