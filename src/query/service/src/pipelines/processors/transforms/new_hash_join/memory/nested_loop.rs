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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;

use crate::pipelines::processors::transforms::JoinStream;

pub struct LoopJoinStream<'a> {
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
