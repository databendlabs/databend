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

pub struct TsvRowSeparator {
    // remain from last read batch
    last_partial_row: Vec<u8>,
    pos: Position,
    record_delimiter: u8,
    in_escape: bool,
    rows_to_skip: u64,
}

/// Like ndjson, we try to pass the buf read to RowBatch (for parallelism) directly, benefits are:
/// - avoid realloc.
/// - able to get the original data for diagnostic.
///
/// TsvRowSeparator is more complicated than NdJsonRowSeparator:
/// delimiter in content can present as '\<delimiter>',
/// which means \n can also present as '\<a_real_newline>' ( the default of mysql)
impl SeparatorState for TsvRowSeparator {
    fn append(&mut self, batch: BytesBatch) -> Result<(Vec<RowBatchWithPosition>, FileStatus)> {
        self.separate(batch)
    }
}

impl TsvRowSeparator {
    pub fn try_create(path: &str, record_delimiter: u8, rows_to_skip: u64) -> Result<Self> {
        Ok(Self {
            last_partial_row: vec![],
            pos: Position::new(path.to_string()),
            record_delimiter,
            in_escape: false,
            rows_to_skip,
        })
    }

    fn separate(
        &mut self,
        mut batch: BytesBatch,
    ) -> Result<(Vec<RowBatchWithPosition>, FileStatus)> {
        let mut data = std::mem::take(&mut batch.data);
        let mut rows: NdjsonRowBatch = Default::default();
        let mut check_first = !self.last_partial_row.is_empty();
        let mut end = 0;
        for (i, b) in data.iter().enumerate() {
            // here we only care about the escape of record_delimiter
            if *b == self.record_delimiter && !self.in_escape {
                if check_first {
                    let mut tail_of_last_batch = std::mem::take(&mut self.last_partial_row);
                    tail_of_last_batch.extend_from_slice(&data[..i]);
                    rows.tail_of_last_batch = Some(tail_of_last_batch);
                    rows.start = i + 1;
                    check_first = false;
                } else if self.rows_to_skip > 0 {
                    self.rows_to_skip -= 1;
                    if self.rows_to_skip == 0 {
                        rows.start = i + 1;
                    }
                } else {
                    rows.row_ends.push(i + 1)
                }
                end = i + 1;
                // keep self.in_escape = false
            } else {
                self.in_escape = (*b == b'\\') && !self.in_escape
            }
        }

        if batch.is_eof {
            if end < data.len() {
                if self.last_partial_row.is_empty() {
                    // make the last line end with record_delimiter,
                    // so we can trim all rows in the same way.
                    // most of the time, it will not lead to realloc.
                    data.push(self.record_delimiter);
                    rows.row_ends.push(data.len());
                } else {
                    self.last_partial_row.extend_from_slice(&data);
                    rows.tail_of_last_batch = Some(std::mem::take(&mut self.last_partial_row));
                }
            }
        } else {
            self.last_partial_row.extend_from_slice(&data[end..]);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn helper(
        last: &[u8],
        new: &[u8],
        is_eof: bool,
        exp_last: &[u8],
        exp_rows: usize,
        exp_output: Option<NdjsonRowBatch>,
    ) -> Result<()> {
        let mut sep = TsvRowSeparator::try_create("test", b'\n', 0).unwrap();
        sep.last_partial_row = last.to_vec();

        let input = BytesBatch {
            data: new.to_vec(),
            path: "".to_string(),
            offset: 0,
            is_eof,
        };

        let (batches, _) = sep.append(input).unwrap();
        assert_eq!(sep.last_partial_row, exp_last);
        assert_eq!(sep.pos.rows, exp_rows);
        if let Some(output) = exp_output {
            assert_eq!(batches.len(), 1);
            if let RowBatch::NDJson(rows) = &batches[0].data {
                assert_eq!(rows.rows(), output.rows());
                assert_eq!(rows.row_ends, output.row_ends);
                assert_eq!(rows.tail_of_last_batch, output.tail_of_last_batch);
            } else {
                panic!()
            }
        } else {
            assert_eq!(batches.len(), 0);
        }
        Ok(())
    }

    // same as NdjsoneRowSeparator
    #[test]
    fn test_tsv_row_separator() -> Result<()> {
        helper(
            b"",
            b"1\n2\n3\n4\n5\n",
            false,
            b"",
            5,
            Some(NdjsonRowBatch {
                data: b"1\n2\n3\n4\n5\n".to_vec(),
                row_ends: vec![2, 4, 6, 8, 10],
                tail_of_last_batch: None,
                start: 0,
            }),
        )?;
        helper(
            b"",
            b"1\n2\n3\n4\n5",
            false,
            b"5",
            4,
            Some(NdjsonRowBatch {
                data: b"1\n2\n3\n4\n5\n".to_vec(),
                row_ends: vec![2, 4, 6, 8],
                tail_of_last_batch: None,
                start: 0,
            }),
        )?;
        helper(
            b"",
            b"1\n2\n3\n4\n5",
            true,
            b"",
            5,
            Some(NdjsonRowBatch {
                data: b"1\n2\n3\n4\n5\n".to_vec(),
                row_ends: vec![2, 4, 6, 8, 10],
                tail_of_last_batch: None,
                start: 0,
            }),
        )?;
        helper(
            b"0",
            b"1\n2\n3\n4\n5\n",
            true,
            b"",
            5,
            Some(NdjsonRowBatch {
                data: b"1\n2\n3\n4\n5\n".to_vec(),
                row_ends: vec![4, 6, 8, 10],
                tail_of_last_batch: Some(b"01".to_vec()),
                start: 2,
            }),
        )?;
        helper(b"0", b"1", false, b"01", 0, None)?;
        helper(
            b"0",
            b"1",
            true,
            b"",
            1,
            Some(NdjsonRowBatch {
                data: b"".to_vec(),
                row_ends: vec![],
                tail_of_last_batch: Some(b"01".to_vec()),
                start: 0,
            }),
        )?;
        Ok(())
    }

    #[test]
    fn test_tsv_row_separator_newline() -> Result<()> {
        helper(
            b"",
            b"\\\n\n2\n3\n4\n5\n",
            false,
            b"",
            5,
            Some(NdjsonRowBatch {
                data: b"\\\n\n2\n3\n4\n5\n".to_vec(),
                row_ends: vec![3, 5, 7, 9, 11],
                tail_of_last_batch: None,
                start: 0,
            }),
        )?;
        Ok(())
    }
}
