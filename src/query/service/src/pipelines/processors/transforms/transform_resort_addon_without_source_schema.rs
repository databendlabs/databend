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

use std::collections::HashMap;
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
use databend_common_expression::SourceSchemaIndex;
use databend_common_expression::ROW_VERSION_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::parse_exprs;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::UnMatchedExprs;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::sessions::QueryContext;

// this processor will receive 3 kinds of blocks:
// 1. from update, in this case, the block has entire target_table schema, no need to do anything
// 2. from insert, we should fill the default values
// 3. from target_build_optimization, we also have entire target_table schema, no need to do anything
// so for 1 and 3, the block's meta is none.
// but for 2, we have source_schema_index
pub struct TransformResortAddOnWithoutSourceSchema {
    expression_transforms: Vec<Option<CompoundBlockOperator>>,
    // data_schemas[i] means the i-th op's result block's schema.
    data_schemas: HashMap<usize, DataSchemaRef>,
    trigger_non_null_errors: Vec<Option<ErrorCode>>,
    // for update block and target_build_optimization block,
    // if target_table has computed expr, we need this.
    computed_expression_transform: CompoundBlockOperator,
    target_table_schema_with_computed: DataSchemaRef,
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

                // The type of stream column _row_version is UInt64, and the default value is 0.
                // skip check for _row_version.
                if !f.is_nullable() && f.name() != ROW_VERSION_COL_NAME {
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
    pub fn try_new(
        ctx: Arc<QueryContext>,
        output_schema: DataSchemaRef,
        unmatched: UnMatchedExprs,
        table: Arc<dyn Table>,
        target_table_schema_with_computed: DataSchemaRef,
    ) -> Result<Self> {
        let mut expression_transforms = Vec::with_capacity(unmatched.len());
        let mut data_schemas = HashMap::with_capacity(unmatched.len());
        let mut trigger_non_null_errors = Vec::with_capacity(unmatched.len());
        for (idx, item) in unmatched.iter().enumerate() {
            let input_schema = item.0.clone();
            data_schemas.insert(idx, input_schema.clone());
            match build_expression_transform(
                input_schema,
                output_schema.clone(),
                table.clone(),
                ctx.clone(),
            ) {
                Ok(expression_transform) => {
                    expression_transforms.push(Some(expression_transform));
                    trigger_non_null_errors.push(None);
                }
                Err(err) => {
                    if err.code() != ErrorCode::BAD_ARGUMENTS {
                        return Err(err);
                    }

                    expression_transforms.push(None);
                    trigger_non_null_errors.push(Some(err));
                }
            };
        }
        // computed_expression_transform will hold entire schema, so this won't get non-null constraint
        let computed_expression_transform = build_expression_transform(
            target_table_schema_with_computed.clone(),
            output_schema,
            table.clone(),
            ctx,
        )?;
        Ok(Self {
            data_schemas,
            expression_transforms,
            trigger_non_null_errors,
            computed_expression_transform,
            target_table_schema_with_computed,
        })
    }
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        unmatched: UnMatchedExprs,
        table: Arc<dyn Table>,
        target_table_schema_with_computed: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let mut expression_transforms = Vec::with_capacity(unmatched.len());
        let mut data_schemas = HashMap::with_capacity(unmatched.len());
        let mut trigger_non_null_errors = Vec::with_capacity(unmatched.len());
        for (idx, item) in unmatched.iter().enumerate() {
            let input_schema = item.0.clone();
            data_schemas.insert(idx, input_schema.clone());
            match build_expression_transform(
                input_schema,
                output_schema.clone(),
                table.clone(),
                ctx.clone(),
            ) {
                Ok(expression_transform) => {
                    expression_transforms.push(Some(expression_transform));
                    trigger_non_null_errors.push(None);
                }
                Err(err) => {
                    if err.code() != ErrorCode::BAD_ARGUMENTS {
                        return Err(err);
                    }

                    expression_transforms.push(None);
                    trigger_non_null_errors.push(Some(err));
                }
            };
        }
        // computed_expression_transform will hold entire schema, so this won't get non-null constraint
        let computed_expression_transform = build_expression_transform(
            target_table_schema_with_computed.clone(),
            output_schema,
            table.clone(),
            ctx,
        )?;
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            Self {
                data_schemas,
                expression_transforms,
                trigger_non_null_errors,
                computed_expression_transform,
                target_table_schema_with_computed,
            },
        )))
    }
}

impl Transform for TransformResortAddOnWithoutSourceSchema {
    const NAME: &'static str = "AddOnWithoutSourceSchemaTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        // see the comment details of `TransformResortAddOnWithoutSourceSchema`.
        if block.get_meta().is_none() {
            block = self.computed_expression_transform.transform(block)?;
            let columns =
                block.columns()[self.target_table_schema_with_computed.num_fields()..].to_owned();
            return Ok(DataBlock::new(columns, block.num_rows()));
        }
        let input_schema_idx =
            SourceSchemaIndex::downcast_from(block.clone().get_owned_meta().unwrap()).unwrap();
        if self.trigger_non_null_errors[input_schema_idx].is_some() {
            let error_code = self.trigger_non_null_errors[input_schema_idx]
                .clone()
                .unwrap();
            return Err(error_code);
        }
        assert!(self.expression_transforms[input_schema_idx].is_some());
        block = self.expression_transforms[input_schema_idx]
            .as_mut()
            .unwrap()
            .transform(block)?;
        let input_schema = self.data_schemas.get(&input_schema_idx).unwrap();
        let columns = block.columns()[input_schema.num_fields()..].to_owned();
        Ok(DataBlock::new(columns, block.num_rows()))
    }
}
