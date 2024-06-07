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

use std::any::Any;
use std::sync::Arc;

use async_channel::Receiver;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_sql::IndexType;

pub struct TransformMergeBlock {
    finished: bool,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    func_ctx: FunctionContext,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    left_schema: DataSchemaRef,
    right_schema: DataSchemaRef,

    left_outputs: Vec<(IndexType, Option<Expr>)>,
    right_outputs: Vec<(IndexType, Option<Expr>)>,

    receiver: Receiver<DataBlock>,
    receiver_result: Option<DataBlock>,
}

impl TransformMergeBlock {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        left_schema: DataSchemaRef,
        right_schema: DataSchemaRef,
        left_outputs: Vec<(IndexType, Option<Expr>)>,
        right_outputs: Vec<(IndexType, Option<Expr>)>,
        receiver: Receiver<DataBlock>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(TransformMergeBlock {
            finished: false,
            input,
            output,
            func_ctx: ctx.get_function_context()?,
            input_data: None,
            output_data: None,
            left_schema,
            right_schema,
            left_outputs,
            receiver,
            receiver_result: None,
            right_outputs,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for TransformMergeBlock {
    fn name(&self) -> String {
        "TransformMergeBlock".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() || self.receiver_result.is_some() {
            return Ok(Event::Sync);
        }

        if let Ok(result) = self.receiver.try_recv() {
            self.receiver_result = Some(result);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if !self.finished {
                return Ok(Event::Async);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_data.take() {
            let left_data = project_block(
                &self.func_ctx,
                input_data,
                &self.left_schema,
                &self.right_schema,
                &self.left_outputs,
                &self.right_outputs,
                true,
            )?;
            if let Some(receiver_result) = self.receiver_result.take() {
                self.output_data = Some(DataBlock::concat(&[
                    left_data,
                    project_block(
                        &self.func_ctx,
                        receiver_result,
                        &self.left_schema,
                        &self.right_schema,
                        &self.left_outputs,
                        &self.right_outputs,
                        false,
                    )?,
                ])?);
            } else {
                self.output_data = Some(left_data);
            }
        } else if let Some(receiver_result) = self.receiver_result.take() {
            self.output_data = Some(project_block(
                &self.func_ctx,
                receiver_result,
                &self.left_schema,
                &self.right_schema,
                &self.left_outputs,
                &self.right_outputs,
                false,
            )?);
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if !self.finished {
            if let Ok(result) = self.receiver.recv().await {
                self.receiver_result = Some(result);
                return Ok(());
            }
            self.finished = true;
        }
        Ok(())
    }
}

pub fn project_block(
    func_ctx: &FunctionContext,
    block: DataBlock,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
    left_outputs: &[(IndexType, Option<Expr>)],
    right_outputs: &[(IndexType, Option<Expr>)],
    is_left: bool,
) -> Result<DataBlock> {
    let num_rows = block.num_rows();
    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let columns = left_outputs
        .iter()
        .zip(right_outputs.iter())
        .map(|(left, right)| {
            if is_left {
                if let Some(expr) = &left.1 {
                    let column = BlockEntry::new(expr.data_type().clone(), evaluator.run(expr)?);
                    Ok(column)
                } else {
                    Ok(block
                        .get_by_offset(left_schema.index_of(&left.0.to_string())?)
                        .clone())
                }
            } else if let Some(expr) = &right.1 {
                let column = BlockEntry::new(expr.data_type().clone(), evaluator.run(expr)?);
                Ok(column)
            } else if left.1.is_some() {
                Ok(block
                    .get_by_offset(right_schema.index_of(&right.0.to_string())?)
                    .clone())
            } else {
                check_type(
                    &left.0.to_string(),
                    &right.0.to_string(),
                    &block,
                    left_schema,
                    right_schema,
                )
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DataBlock::new(columns, num_rows))
}

fn check_type(
    left_name: &str,
    right_name: &str,
    block: &DataBlock,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
) -> Result<BlockEntry> {
    let left_field = left_schema.field_with_name(left_name)?;
    let left_data_type = left_field.data_type();

    let right_field = right_schema.field_with_name(right_name)?;
    let right_data_type = right_field.data_type();

    let index = right_schema.index_of(right_name)?;

    if left_data_type == right_data_type {
        return Ok(block.get_by_offset(index).clone());
    }

    if left_data_type.remove_nullable() == right_data_type.remove_nullable() {
        let origin_column = block.get_by_offset(index).clone();
        let mut builder = ColumnBuilder::with_capacity(left_data_type, block.num_rows());
        let value = origin_column.value.as_ref();
        for idx in 0..block.num_rows() {
            let scalar = value.index(idx).unwrap();
            builder.push(scalar);
        }
        let col = builder.build();
        Ok(BlockEntry::new(left_data_type.clone(), Value::Column(col)))
    } else {
        Err(ErrorCode::IllegalDataType(
            "The data type on both sides of the union does not match",
        ))
    }
}
