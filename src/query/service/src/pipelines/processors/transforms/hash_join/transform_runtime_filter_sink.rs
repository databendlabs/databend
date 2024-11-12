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
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use crate::pipelines::processors::HashJoinState;

pub struct TransformRuntimeFilterSink {
    input: Arc<InputPort>,
    hash_join_state: Arc<HashJoinState>,
    num_collected_nodes: usize,
    num_cluster_nodes: usize,
    is_collected: HashMap<String, bool>,
    data_blocks: Vec<DataBlock>,
}

impl TransformRuntimeFilterSink {
    pub fn create(
        input: Arc<InputPort>,
        hash_join_state: Arc<HashJoinState>,
        num_cluster_nodes: usize,
        is_collected: HashMap<String, bool>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformRuntimeFilterSink {
            input,
            hash_join_state,
            num_collected_nodes: 0,
            num_cluster_nodes,
            is_collected,
            data_blocks: Vec::new(),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for TransformRuntimeFilterSink {
    fn name(&self) -> String {
        String::from("TransformRuntimeFilterSink")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.input.is_finished() {
            self.data_blocks.clear();
            return Ok(Event::Finished);
        } else if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;
            let num_columns = data_block.num_columns();
            let node_id_value = data_block.get_by_offset(num_columns - 2).value.clone();
            let need_to_build_value = data_block.get_by_offset(num_columns - 1).value.clone();
            let node_id = node_id_value.index(0).unwrap().into_string().unwrap();
            let need_to_build = need_to_build_value
                .index(0)
                .unwrap()
                .into_boolean()
                .unwrap();
            if need_to_build {
                if let Some(is_collected) = self.is_collected.get_mut(node_id) {
                    if !*is_collected {
                        self.num_collected_nodes += 1;
                        *is_collected = true;
                    }
                }
                let num_columns = data_block.num_columns() - 2;
                self.data_blocks.push(data_block);
                if self.num_collected_nodes == self.num_cluster_nodes {
                    let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
                    let bloom_filter_columns = &mut build_state.runtime_filter_columns;
                    for column_index in 0..num_columns {
                        let mut columns = Vec::new();
                        for data_block in self.data_blocks.iter() {
                            let num_rows = data_block.num_rows();
                            columns
                                .push(data_block.get_by_offset(column_index).to_column(num_rows));
                        }
                        bloom_filter_columns.push(Column::concat_columns(columns.into_iter())?);
                    }
                    self.hash_join_state
                        .is_runtime_filter_data_ready
                        .store(true, Ordering::Release);
                }
            } else {
                self.hash_join_state
                    .is_runtime_filter_data_ready
                    .store(true, Ordering::Release);
            }
        }
        self.input.set_need_data();
        Ok(Event::NeedData)
    }
}
