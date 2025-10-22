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

use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;

use crate::Transform;
use crate::Transformer;

pub struct TransformCastSchema {
    func_ctx: FunctionContext,
    to_schema: DataSchemaRef,
    from_schema: DataSchemaRef,
    exprs: Vec<Expr>,
}

impl TransformCastSchema
where Self: Transform
{
    pub fn try_new(
        select_schema: DataSchemaRef,
        insert_schema: DataSchemaRef,
        func_ctx: FunctionContext,
    ) -> Result<Self> {
        let exprs = build_cast_exprs(select_schema.clone(), insert_schema.clone())?;

        Ok(Self {
            func_ctx,
            to_schema: insert_schema,
            from_schema: select_schema,
            exprs,
        })
    }

    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        select_schema: DataSchemaRef,
        insert_schema: DataSchemaRef,
        func_ctx: FunctionContext,
    ) -> Result<ProcessorPtr> {
        let me = Self::try_new(select_schema, insert_schema, func_ctx)?;
        Ok(ProcessorPtr::create(Transformer::create(
            input_port,
            output_port,
            me,
        )))
    }
}

impl Transform for TransformCastSchema {
    const NAME: &'static str = "CastSchemaTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        cast_schema(
            data_block,
            self.from_schema.clone(),
            self.to_schema.clone(),
            &self.exprs,
            &self.func_ctx,
        )
    }
}

pub fn build_cast_exprs(from_schema: DataSchemaRef, to_schema: DataSchemaRef) -> Result<Vec<Expr>> {
    from_schema
        .fields()
        .iter()
        .zip(to_schema.fields().iter().enumerate())
        .map(|(from, (index, to))| {
            let expr = ColumnRef {
                span: None,
                id: index,
                data_type: from.data_type().clone(),
                display_name: from.name().clone(),
            };
            check_cast(None, false, expr.into(), to.data_type(), &BUILTIN_FUNCTIONS)
        })
        .collect::<Result<Vec<_>>>()
}

pub fn cast_schema(
    data_block: DataBlock,
    from_schema: DataSchemaRef,
    to_schema: DataSchemaRef,
    exprs: &[Expr],
    func_ctx: &FunctionContext,
) -> Result<DataBlock> {
    let mut entries = Vec::with_capacity(exprs.len());
    let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);
    for (i, (field, expr)) in to_schema.fields().iter().zip(exprs.iter()).enumerate() {
        let value = evaluator.run(expr).map_err(|err| {
            let msg = format!(
                "fail to auto cast column {} ({}) to column {} ({})",
                from_schema.fields[i].name(),
                from_schema.fields[i].data_type(),
                field.name(),
                field.data_type(),
            );
            err.add_message(msg)
        })?;
        let entry = BlockEntry::new(value, || (field.data_type().clone(), data_block.num_rows()));
        entries.push(entry);
    }
    Ok(DataBlock::new(entries, data_block.num_rows()))
}
