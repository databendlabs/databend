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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::parse_exprs;
use databend_common_storages_factory::Table;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::sessions::QueryContext;

pub struct TransformResortAddOnWithoutSourceSchema {
    output_schema: DataSchemaRef,
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
}

pub fn build_expression_transform(
    input_schema: DataSchemaRef,
    output_schema: DataSchemaRef,
    table: Arc<dyn Table>,
    ctx: Arc<QueryContext>,
) -> Result<CompoundBlockOperator> {
    let mut exprs = Vec::with_capacity(output_schema.fields().len());
    for f in output_schema.fields().iter() {
        let expr = if !input_schema.has_field(f.name()) {
            if let Some(default_expr) = f.default_expr() {
                let expr = parse_exprs(ctx.clone(), table.clone(), default_expr)?.remove(0);
                check_cast(None, false, expr, f.data_type(), &BUILTIN_FUNCTIONS)?
            } else {
                // #issue13932
                // if there is a non-null constraint, we should return an error
                // although we will give a valid default value. for example:
                // 1. (a int not null), we give zero
                // 2. (a int), we give null
                // but for pg or snowflake, if a field is non-null, it will return
                // a non-null error (it means they will give a null as the default value).
                if !f.is_nullable() {
                    // if we have a user-specified default expr, it must satisfy the non-null constraint
                    // in table-create phase. So we just consider default_expr is none.
                    return Err(ErrorCode::BadArguments(format!(
                        "null value in column `{}` of table `{}` violates not-null constraint",
                        f.name(),
                        table.name()
                    )));
                }
                let default_value = Scalar::default_value(f.data_type());
                Expr::Constant {
                    span: None,
                    scalar: default_value,
                    data_type: f.data_type().clone(),
                }
            }
        } else {
            let field = input_schema.field_with_name(f.name()).unwrap();
            let id = input_schema.index_of(f.name()).unwrap();
            Expr::ColumnRef {
                span: None,
                id,
                data_type: field.data_type().clone(),
                display_name: field.name().clone(),
            }
        };
        exprs.push(expr);
    }

    let func_ctx = ctx.get_function_context()?;
    Ok(CompoundBlockOperator {
        ctx: func_ctx,
        operators: vec![BlockOperator::Map {
            exprs,
            projections: None,
        }],
    })
}

impl TransformResortAddOnWithoutSourceSchema
where Self: Transform
{
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        table: Arc<dyn Table>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            Self {
                output_schema,
                ctx,
                table,
            },
        )))
    }
}

impl Transform for TransformResortAddOnWithoutSourceSchema {
    const NAME: &'static str = "AddOnWithoutSourceSchemaTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let input_schema =
            DataSchemaRef::downcast_from(block.clone().get_owned_meta().unwrap()).unwrap();
        block = build_expression_transform(
            input_schema.clone(),
            self.output_schema.clone(),
            self.table.clone(),
            self.ctx.clone(),
        )?
        .transform(block)?;
        let columns = block.columns()[input_schema.num_fields()..].to_owned();
        Ok(DataBlock::new(columns, block.num_rows()))
    }
}
