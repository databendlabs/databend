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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use crate::pipelines::processors::HashJoinState;

pub struct TransformRuntimeFilterSource {
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    node_id: String,
    hash_join_state: Arc<HashJoinState>,
    is_runtime_filter_readed: bool,
}

impl TransformRuntimeFilterSource {
    pub fn create(
        output: Arc<OutputPort>,
        node_id: String,
        hash_join_state: Arc<HashJoinState>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(
            TransformRuntimeFilterSource {
                output,
                output_data: None,
                node_id,
                hash_join_state,
                is_runtime_filter_readed: false,
            },
        )))
    }
}

impl TransformRuntimeFilterSource {
    #[async_backtrace::framed]
    async fn wait_runtime_filter_notify(&mut self) -> Result<bool> {
        let mut rx = self
            .hash_join_state
            .build_runtime_filter_watcher
            .subscribe();
        if (*rx.borrow()).is_some() {
            return Ok((*rx.borrow()).unwrap());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("watcher's sender is dropped"))?;
        let need_to_build_runtime_filter = (*rx.borrow()).unwrap();
        Ok(need_to_build_runtime_filter)
    }
}

#[async_trait::async_trait]
impl Processor for TransformRuntimeFilterSource {
    fn name(&self) -> String {
        String::from("TransformRuntimeFilterSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
        }

        if !self.is_runtime_filter_readed {
            Ok(Event::Async)
        } else {
            self.output.finish();
            Ok(Event::Finished)
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let need_to_build_runtime_filter = self.wait_runtime_filter_notify().await?;
        if need_to_build_runtime_filter {
            println!("TransformRuntimeFilterSource: need to build runtime filter");
        }

        let data_block = if need_to_build_runtime_filter {
            let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
            let bloom_filter_columns = std::mem::take(&mut build_state.runtime_filter_columns);
            let mut data_block = DataBlock::new_from_columns(bloom_filter_columns);
            data_block.add_column(BlockEntry::new(
                DataType::String,
                Value::Scalar(Scalar::String(self.node_id.clone())),
            ));
            data_block.add_column(BlockEntry::new(
                DataType::Boolean,
                Value::Scalar(Scalar::Boolean(true)),
            ));
            data_block
        } else {
            let runtime_filter_source_fields = &self.hash_join_state.runtime_filter_source_fields;
            let mut block_entries = Vec::with_capacity(runtime_filter_source_fields.len());
            for field in runtime_filter_source_fields
                .iter()
                .take(runtime_filter_source_fields.len() - 2)
            {
                let data_type = field.data_type().clone();
                let column = Value::Column(Column::random(&data_type, 1, None));
                block_entries.push(BlockEntry::new(data_type, column));
            }
            block_entries.push(BlockEntry::new(
                DataType::String,
                Value::Scalar(Scalar::String(self.node_id.clone())),
            ));
            block_entries.push(BlockEntry::new(
                DataType::Boolean,
                Value::Scalar(Scalar::Boolean(false)),
            ));
            DataBlock::new(block_entries, 1)
        };

        self.output_data = Some(data_block);
        self.is_runtime_filter_readed = true;
        Ok(())
    }
}
