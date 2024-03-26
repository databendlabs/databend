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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::plans::ComparisonOp;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::SubqueryDesc;
use databend_common_sql::plans::SubqueryType;

use crate::operations::check_for_eliminate_valids;
use crate::operations::get_not;

struct SplitMutator {
    pub func_ctx: FunctionContext,
    pub subquery_desc: SubqueryDesc,
    pub eliminate_valids: bool,
}

impl SplitMutator {
    pub fn try_create(func_ctx: FunctionContext, subquery_desc: SubqueryDesc) -> Result<Self> {
        let eliminate_valids = check_for_eliminate_valids(
            subquery_desc.from_correlated_subquery,
            &subquery_desc.subquery_join_type,
        );
        Ok(Self {
            func_ctx,
            subquery_desc,
            eliminate_valids,
        })
    }

    fn get_filter(
        &self,
        predicate: Value<BooleanType>,
        data_block: &DataBlock,
    ) -> Result<(Value<BooleanType>, Value<BooleanType>)> {
        let (predicate_not, _) = get_not(predicate.clone(), &self.func_ctx, data_block.num_rows())?;
        let predicate_not = predicate_not.try_downcast().unwrap();
        Ok((predicate, predicate_not))
    }

    fn eliminate_filter(
        &self,
        data_block: &DataBlock,
    ) -> Result<(Value<BooleanType>, Value<BooleanType>)> {
        let filter_entry = data_block.get_by_offset(data_block.num_columns() - 1);
        let value = if let Value::Column(Column::Nullable(col)) = &filter_entry.value {
            Value::Column(col.column.clone())
        } else {
            return Err(ErrorCode::from_string(
                "subquery filter type MUST be Column::Nullable(Boolean".to_string(),
            ));
        };
        let predicate: Option<Value<BooleanType>> = value.try_downcast();
        if let Some(predicate) = predicate {
            self.get_filter(predicate, data_block)
        } else {
            Err(ErrorCode::from_string(
                "subquery filter type MUST be Column::Nullable(Boolean)".to_string(),
            ))
        }
    }

    pub fn split_not_matched_block(&self, block: DataBlock) -> Result<DataBlock> {
        let subquery_desc = &self.subquery_desc;
        let predicate_with_marker = subquery_desc.predicate_with_marker;
        match &subquery_desc.typ {
            SubqueryType::Exists => {
                if predicate_with_marker {
                    let (_predicate, predicate_not) = if self.eliminate_valids {
                        self.eliminate_filter(&block)?
                    } else {
                        let filter_entry = block.get_by_offset(block.num_columns() - 1);
                        let predicate: Option<Value<BooleanType>> =
                            filter_entry.value.try_downcast();
                        if let Some(predicate) = predicate {
                            self.get_filter(predicate, &block)?
                        } else {
                            return Err(ErrorCode::from_string(
                                "subquery filter type MUST be Column::Boolean".to_string(),
                            ));
                        }
                    };
                    block.filter_boolean_value(&predicate_not)
                } else {
                    Ok(DataBlock::empty())
                }
            }
            SubqueryType::NotExists => {
                if predicate_with_marker {
                    let (predicate, _predicate_not) = if self.eliminate_valids {
                        self.eliminate_filter(&block)?
                    } else {
                        let filter_entry = block.get_by_offset(block.num_columns() - 1);
                        let predicate: Option<Value<BooleanType>> =
                            filter_entry.value.try_downcast();
                        if let Some(predicate) = predicate {
                            self.get_filter(predicate, &block)?
                        } else {
                            return Err(ErrorCode::from_string(
                                "subquery filter type MUST be Column::Boolean".to_string(),
                            ));
                        }
                    };
                    block.filter_boolean_value(&predicate)
                } else {
                    Ok(block)
                }
            }
            SubqueryType::Scalar => {
                return Err(ErrorCode::from_string(
                    "Cannot use Scalar as subquery filter".to_string(),
                ));
            }
            _ => {
                let (predicate, predicate_not) = self.eliminate_filter(&block)?;
                let not_matched_block =
                    if let Some(ComparisonOp::NotEqual) = &subquery_desc.compare_op {
                        block.filter_boolean_value(&predicate)?
                    } else {
                        block.filter_boolean_value(&predicate_not)?
                    };

                let num_columns = not_matched_block.num_columns();
                let not_matched_block = DataBlock::new(
                    not_matched_block.columns()[..num_columns - 1].to_vec(),
                    not_matched_block.num_rows(),
                );
                Ok(not_matched_block)
            }
        }
    }
}

pub enum SubqueryMutation {
    Delete,
    Update(Vec<BlockOperator>),
}

pub struct TransformMutationSubquery {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    mutation: SubqueryMutation,
    split_mutator: SplitMutator,
}

impl TransformMutationSubquery {
    pub fn try_create(
        func_ctx: FunctionContext,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        mutation: SubqueryMutation,
        subquery_desc: SubqueryDesc,
    ) -> Result<Self> {
        let split_mutator = SplitMutator::try_create(func_ctx, subquery_desc)?;

        Ok(TransformMutationSubquery {
            input,
            output,
            input_data: None,
            output_data: None,
            mutation,
            split_mutator,
        })
    }

    pub fn into_processor(self) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(self)))
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input.clone();
        let output = self.output.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output])
    }

    fn eliminate_predicate_valids(&self, data_block: &mut DataBlock) -> Result<()> {
        let subquery_desc = &self.split_mutator.subquery_desc;
        let num_rows = data_block.num_rows();
        let predicate_entry = data_block.get_by_offset(data_block.num_columns() - 1);
        let value = if let Value::Column(Column::Nullable(col)) = &predicate_entry.value {
            Value::Column(col.column.clone())
        } else {
            return Err(ErrorCode::from_string(
                "subquery filter type MUST be Column::Nullable(Boolean".to_string(),
            ));
        };

        let value = if let Some(ComparisonOp::NotEqual) = &subquery_desc.compare_op {
            let predicate: Value<BooleanType> = value.try_downcast().unwrap();
            let (value, _) = get_not(predicate, &self.split_mutator.func_ctx, num_rows)?;
            value
        } else {
            value
        };
        // convert Nullable(Boolean) to Boolean
        data_block.pop_columns(1);
        data_block.add_column(BlockEntry::new(DataType::Boolean, value));

        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for TransformMutationSubquery {
    fn name(&self) -> String {
        "TransformMutationSubquery".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // 1. if there is no data and input_port is finished, this processor has finished
        // it's work
        if self.input.is_finished() && self.output_data.is_none() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        // 2. process data stage here
        if self.output.can_push() {
            if let Some(data) = self.output_data.take() {
                self.output.push_data(Ok(data));
                return Ok(Event::NeedConsume);
            }
        }

        // 3. trigger down stream pipeItem to consume if we pushed data
        if self.input.has_data() {
            if self.output_data.is_none() {
                // no pending data (being sent to down streams)
                self.input_data = Some(self.input.pull_data().unwrap()?);
                return Ok(Event::Sync);
            } else {
                // data pending
                return Ok(Event::NeedConsume);
            }
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(mut data_block) = self.input_data.take() {
            if data_block.is_empty() {
                return Ok(());
            }
            let num_rows = data_block.num_rows();
            let subquery_desc = &self.split_mutator.subquery_desc;
            let eliminate_valids = self.split_mutator.eliminate_valids;

            let output_data = match &self.mutation {
                SubqueryMutation::Delete => {
                    self.split_mutator.split_not_matched_block(data_block)?
                }
                SubqueryMutation::Update(operators) => {
                    match &subquery_desc.typ {
                        SubqueryType::NotExists => {
                            if subquery_desc.predicate_with_marker {
                                if eliminate_valids {
                                    self.eliminate_predicate_valids(&mut data_block)?;
                                }
                            } else {
                                let bitmap = Bitmap::new_constant(false, num_rows);
                                let predicate_entry = BlockEntry::new(
                                    DataType::Boolean,
                                    Value::Column(Column::Boolean(bitmap.into())),
                                );
                                data_block.add_column(predicate_entry);
                            }
                        }
                        SubqueryType::Exists => {
                            if subquery_desc.predicate_with_marker {
                                if eliminate_valids {
                                    self.eliminate_predicate_valids(&mut data_block)?;
                                }
                            } else {
                                let bitmap = Bitmap::new_constant(true, num_rows);
                                let predicate_entry = BlockEntry::new(
                                    DataType::Boolean,
                                    Value::Column(Column::Boolean(bitmap.into())),
                                );
                                data_block.add_column(predicate_entry);
                            }
                        }
                        SubqueryType::Scalar => {
                            return Err(ErrorCode::from_string(
                                "Cannot use Scalar as subquery filter".to_string(),
                            ));
                        }
                        _ => {
                            self.eliminate_predicate_valids(&mut data_block)?;
                        }
                    };

                    operators.iter().try_fold(data_block, |input, op| {
                        op.execute(&self.split_mutator.func_ctx, input)
                    })?
                }
            };
            self.output_data = Some(output_data);
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        Ok(())
    }
}
