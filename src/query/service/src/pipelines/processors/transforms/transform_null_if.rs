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
use databend_common_expression::types::NumberScalar;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnIndex;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use futures_util::StreamExt;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::ProcessorPtr;

pub struct TransformNullIf {
    func_ctx: FunctionContext,
    insert_schema: DataSchemaRef,
    exprs: Vec<Expr>,
}

impl TransformNullIf
where Self: Transform
{
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        select_schema: DataSchemaRef,
        insert_schema: DataSchemaRef,
        func_ctx: FunctionContext,
        null_str_list: &[String],
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
                    display_name: from.name().clone(),
                };
                Self::try_null_if(
                    None,
                    expr,
                    to.data_type(),
                    &BUILTIN_FUNCTIONS,
                    null_str_list,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ProcessorPtr::create(Transformer::create(
            input_port,
            output_port,
            Self {
                func_ctx,
                insert_schema,
                exprs,
            },
        )))
    }

    fn datatype_need_transform(ty: &DataType) -> bool {
        match ty {
            DataType::String => true,
            DataType::Nullable(b) if matches!(**b, DataType::String) => true,
            _ => false,
        }
    }

    fn new_schema(schema: &mut DataSchema) {
        for field in &mut schema.fields {
            if let DataType::String = field.data_type() {
                *field =
                    DataField::new(field.name(), DataType::Nullable(Box::new(DataType::String)))
            }
        }
    }

    fn try_null_if<Index: ColumnIndex>(
        span: Span,
        expr: Expr<Index>,
        dest_type: &DataType,
        fn_registry: &FunctionRegistry,
        null_str_list: &[String],
    ) -> Result<Expr<Index>> {
        let src_type = expr.data_type();
        let mut args = null_str_list
            .iter()
            .map(|s| Expr::Constant {
                span,
                scalar: Scalar::String(s.clone()),
                data_type: DataType::String,
            })
            .collect::<Vec<_>>();
        args.push(expr.clone());
        if dest_type.is_nullable() && Self::datatype_need_transform(src_type) {
            let in_expr = check_function(span, "contains", &[], &args, fn_registry)?;
            let if_expr = check_function(
                span,
                "if",
                &[],
                &[in_expr, Expr::Constant {
                    span,
                    scalar: Scalar::Null,
                    data_type: DataType::Nullable(Box::new(DataType::String)),
                }],
                fn_registry,
            )?;
            Ok(if_expr)
        } else {
            Ok(expr)
        }
    }
}

impl Transform for TransformNullIf {
    const NAME: &'static str = "NullIfTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let mut columns = Vec::with_capacity(self.exprs.len());
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        for (field, expr) in self.insert_schema.fields().iter().zip(self.exprs.iter()) {
            let value = evaluator.run(expr)?;
            let column = BlockEntry::new(field.data_type().clone(), value);
            columns.push(column);
        }
        Ok(DataBlock::new(columns, data_block.num_rows()))
    }
}
