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

use std::default::Default;

use databend_common_exception::Result;
use databend_common_storage::FileStatus;

use crate::read::row_based::batch::BytesBatch;
use crate::read::row_based::batch::NdjsonRowBatch;
use crate::read::row_based::batch::Position;
use crate::read::row_based::batch::RowBatch;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::SeparatorState;

pub struct NdJsonRowSeparator {
    // remain from last read batch
    last_partial_row: Vec<u8>,
    pos: Position,
}

impl SeparatorState for NdJsonRowSeparator {
    fn append(&mut self, batch: BytesBatch) -> Result<(Vec<RowBatchWithPosition>, FileStatus)> {
        self.separate(batch)
    }
}

impl NdJsonRowSeparator {
    pub fn try_create(path: &str) -> Result<Self> {
        Ok(Self {
            last_partial_row: vec![],
            pos: Position::new(path.to_string()),
        })
    }

    fn separate(
        &mut self,
        mut batch: BytesBatch,
    ) -> Result<(Vec<RowBatchWithPosition>, FileStatus)> {
        let data = std::mem::take(&mut batch.data);
        let mut rows: NdjsonRowBatch = Default::default();
        let mut start = 0;
        let mut check_first = !self.last_partial_row.is_empty();
        for (i, b) in data.iter().enumerate() {
            if *b == b'\n' {
                if check_first {
                    rows.tail_of_last_batch = std::mem::take(&mut self.last_partial_row);
                    rows.tail_of_last_batch.extend_from_slice(&data[..i]);
                    start = i;
                    rows.start = start;
                    check_first = false;
                } else {
                    rows.row_ends.push(i + 1 + start)
                }
            }
        }

        if batch.is_eof && data.last() != Some(&b'\n') {
            if self.last_partial_row.is_empty() {
                rows.row_ends.push(data.len())
            } else {
                self.last_partial_row.extend_from_slice(&data);
                rows.tail_of_last_batch = std::mem::take(&mut self.last_partial_row);
            }
        }

        let batch = if rows.rows() == 0 {
            vec![]
        } else {
            rows.data = data;
            let out_pos = self.pos.clone();
            self.pos.rows += rows.rows();
            vec![RowBatchWithPosition::new(RowBatch::NDJson(rows), out_pos)]
        };
        Ok((batch, FileStatus::default()))
    }
}
