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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;

use super::inner_join::InnerHashJoinFilterStream;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::JoinStream;
use crate::pipelines::processors::transforms::NestedLoopDesc;
use crate::pipelines::processors::transforms::RuntimeFiltersDesc;

pub struct NestedLoopJoin<T> {
    inner: T,
    basic_state: Arc<BasicHashJoinState>,
    desc: NestedLoopDesc,
}

impl<T> NestedLoopJoin<T> {
    pub fn create(inner: T, basic_state: Arc<BasicHashJoinState>, desc: NestedLoopDesc) -> Self {
        Self {
            inner,
            basic_state,
            desc,
        }
    }
}

impl<T: Join> Join for NestedLoopJoin<T> {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.inner.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.inner.final_build()
    }

    fn build_runtime_filter(&self, desc: &RuntimeFiltersDesc) -> Result<JoinRuntimeFilterPacket> {
        self.inner.build_runtime_filter(desc)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || *self.basic_state.build_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }
        let HashJoinHashTable::NestedLoop(build_blocks) = &*self.basic_state.hash_table else {
            return self.inner.probe_block(data);
        };

        let nested = Box::new(LoopJoinStream::new(data, build_blocks));
        Ok(InnerHashJoinFilterStream::create(
            nested,
            &mut self.desc.filter,
            self.desc.field_reorder.as_deref(),
        ))
    }
}

struct LoopJoinStream<'a> {
    probe_rows: VecDeque<Vec<Scalar>>,
    probe_types: Vec<DataType>,
    build_blocks: &'a [DataBlock],
    build_index: usize,
}

impl<'a> LoopJoinStream<'a> {
    pub fn new(probe: DataBlock, build_blocks: &'a [DataBlock]) -> Self {
        let mut probe_rows = vec![Vec::new(); probe.num_rows()];
        for entry in probe.columns().iter() {
            match entry {
                BlockEntry::Const(scalar, _, _) => {
                    for row in &mut probe_rows {
                        row.push(scalar.to_owned());
                    }
                }
                BlockEntry::Column(column) => {
                    for (row, scalar) in probe_rows.iter_mut().zip(column.iter()) {
                        row.push(scalar.to_owned());
                    }
                }
            }
        }

        let left_types = probe
            .columns()
            .iter()
            .map(|entry| entry.data_type())
            .collect();

        LoopJoinStream {
            probe_rows: probe_rows.into(),
            probe_types: left_types,
            build_blocks,
            build_index: 0,
        }
    }
}

impl<'a> JoinStream for LoopJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let Some(probe_entries) = self.probe_rows.front() else {
            return Ok(None);
        };

        let build_block = &self.build_blocks[self.build_index];

        let entries = probe_entries
            .iter()
            .zip(self.probe_types.iter())
            .map(|(scalar, data_type)| {
                BlockEntry::Const(scalar.clone(), data_type.clone(), build_block.num_rows())
            })
            .chain(build_block.columns().iter().cloned())
            .collect();

        self.build_index += 1;
        if self.build_index >= self.build_blocks.len() {
            self.build_index = 0;
            self.probe_rows.pop_front();
        }

        Ok(Some(DataBlock::new(entries, build_block.num_rows())))
    }
}
