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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::binder::MergeIntoType;

use super::hash_join::HashJoinBuildState;
use super::processor_deduplicate_row_number::get_row_number;
pub struct ExtractHashTableByRowNumber {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Vec<DataBlock>,
    probe_data_fields: Vec<DataField>,
    hashstate: Arc<HashJoinBuildState>,
    // if insert only, we don't need to
    // fill null BlockEntries
    merge_type: MergeIntoType,
}

impl ExtractHashTableByRowNumber {
    pub fn create(
        hashstate: Arc<HashJoinBuildState>,
        probe_data_fields: Vec<DataField>,
        merge_type: MergeIntoType,
    ) -> Result<Self> {
        Ok(Self {
            input_port: InputPort::create(),
            output_port: OutputPort::create(),
            hashstate,
            probe_data_fields,
            input_data: None,
            output_data: Vec::new(),
            merge_type,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port = self.output_port.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output_port])
    }
}

impl Processor for ExtractHashTableByRowNumber {
    fn name(&self) -> String {
        "ExtractHashTableByRowNumber".to_owned()
    }

    #[doc = " Reference used for downcast."]
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let finished = self.input_port.is_finished() && self.output_data.is_empty();
        if finished {
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;

        if self.output_port.can_push() && !self.output_data.is_empty() {
            self.output_port
                .push_data(Ok(self.output_data.pop().unwrap()));
            pushed_something = true
        }

        if pushed_something {
            return Ok(Event::NeedConsume);
        }

        if self.input_port.has_data() {
            if self.output_data.is_empty() {
                self.input_data = Some(self.input_port.pull_data().unwrap()?);
                Ok(Event::Sync)
            } else {
                Ok(Event::NeedConsume)
            }
        } else {
            self.input_port.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            if data_block.is_empty() {
                merge_into_distributed_hashtable_empty_block(1);
                return Ok(());
            }

            merge_into_distributed_hashtable_fetch_row_number(data_block.num_rows() as u32);
            let row_number_vec = get_row_number(&data_block, 0);
            let length = row_number_vec.len();
            let row_number_set: HashSet<u64> = row_number_vec.into_iter().collect();
            assert_eq!(row_number_set.len(), length);

            // get datablocks from hashstate.
            unsafe {
                let build_state = &*self.hashstate.hash_join_state.build_state.get();
                for block in build_state.generation_state.chunks.iter() {
                    assert_eq!(
                        block.columns()[block.num_columns() - 1].data_type,
                        DataType::Number(NumberDataType::UInt64)
                    );
                    let row_numbers = get_row_number(block, block.num_columns() - 1);
                    let mut bitmap = MutableBitmap::with_capacity(row_numbers.len());
                    for row_number in row_numbers.iter() {
                        if row_number_set.contains(row_number) {
                            bitmap.push(true);
                        } else {
                            bitmap.push(false);
                        }
                    }
                    let filtered_block = block.clone().filter_with_bitmap(&bitmap.into())?;
                    let res_block = if let MergeIntoType::InsertOnly = self.merge_type {
                        filtered_block
                    } else {
                        // Create null chunk for unmatched rows in probe side
                        let mut null_block = DataBlock::new(
                            self.probe_data_fields
                                .iter()
                                .map(|df| {
                                    BlockEntry::new(
                                        df.data_type().clone(),
                                        Value::Scalar(Scalar::Null),
                                    )
                                })
                                .collect(),
                            filtered_block.num_rows(),
                        );
                        null_block.merge_block(filtered_block);
                        null_block
                    };

                    if res_block.is_empty() {
                        merge_into_distributed_hashtable_push_empty_null_block(1);
                    } else {
                        merge_into_distributed_hashtable_push_null_block(1);
                        merge_into_distributed_hashtable_push_null_block_rows(
                            res_block.num_rows() as u32
                        );
                    }
                    self.output_data.push(res_block);
                }
            }
        }
        Ok(())
    }
}
