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

use std::cmp::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::utils::sort_key_order::SortKeyEvaluator;
use databend_common_expression::utils::sort_key_order::SortKeyRow;
use databend_common_expression::utils::sort_key_order::compare_sort_key_rows;
use databend_common_expression::utils::sort_key_order::display_sort_key_row;
use databend_common_functions::BUILTIN_FUNCTIONS;

/// Builds the blocks of a COPY with `FILE_SORTED_BY` while one source reads a whole file:
/// consecutive blocks of the file are concatenated into blocks of the target size, and the
/// key ranges of the read blocks are checked not to go back, taking each range from the first
/// and last rows. A block never spans two files.
pub(crate) struct FileOrder {
    sort_keys: SortKeyEvaluator,
    thresholds: BlockThresholds,
    location: String,
    rows_read: usize,
    /// The sort key of the last row of the current file so far.
    last_key: Option<SortKeyRow>,
    blocks: Vec<DataBlock>,
    rows: usize,
    bytes: usize,
}

impl FileOrder {
    pub(crate) fn try_create(
        keys: &[RemoteExpr<String>],
        output_schema: &DataSchema,
        func_ctx: FunctionContext,
        thresholds: BlockThresholds,
    ) -> Result<Self> {
        Ok(Self {
            sort_keys: SortKeyEvaluator::try_create(
                keys,
                output_schema,
                func_ctx,
                &BUILTIN_FUNCTIONS,
            )?,
            thresholds,
            location: String::new(),
            rows_read: 0,
            last_key: None,
            blocks: Vec::new(),
            rows: 0,
            bytes: 0,
        })
    }

    /// Adds the next block of `location`, returning a block once enough rows are collected.
    pub(crate) fn add(&mut self, location: &str, block: DataBlock) -> Result<Option<DataBlock>> {
        let mut output = None;
        if self.location != location {
            output = self.flush()?;
            self.location = location.to_string();
            self.rows_read = 0;
            self.last_key = None;
        }
        if block.is_empty() {
            return Ok(output);
        }

        // The rows of a block are sorted again on write, so only the ranges matter: each block
        // must start at or after the end of the block before it in the same file.
        if let Some((first, last)) = self.sort_keys.boundary_keys(&block, &BUILTIN_FUNCTIONS)? {
            if compare_sort_key_rows(&first, &last) == Ordering::Greater {
                return Err(self.violation(
                    self.rows_read + 1,
                    format!(
                        "the rows from here start at {}, above {} where they end",
                        display_sort_key_row(&first),
                        display_sort_key_row(&last),
                    ),
                ));
            }
            if let Some(previous) = &self.last_key
                && compare_sort_key_rows(&first, previous) == Ordering::Less
            {
                return Err(self.violation(
                    self.rows_read + 1,
                    format!(
                        "the rows from here start at {}, below {} of the rows before",
                        display_sort_key_row(&first),
                        display_sort_key_row(previous),
                    ),
                ));
            }
            self.last_key = Some(last);
        }
        self.rows_read += block.num_rows();

        self.rows += block.num_rows();
        self.bytes += block.estimate_block_size(block.num_columns());
        self.blocks.push(block);
        // The source flushes at the end of every file, so `output` is normally empty here.
        if output.is_none() && self.thresholds.check_large_enough(self.rows, self.bytes) {
            output = self.flush()?;
        }
        Ok(output)
    }

    /// Returns the collected rows of the current file as one block.
    pub(crate) fn flush(&mut self) -> Result<Option<DataBlock>> {
        self.rows = 0;
        self.bytes = 0;
        match self.blocks.len() {
            0 => Ok(None),
            1 => Ok(self.blocks.pop()),
            _ => {
                let block = DataBlock::concat(&self.blocks)?;
                self.blocks.clear();
                Ok(Some(block))
            }
        }
    }

    fn violation(&self, row: usize, detail: String) -> ErrorCode {
        ErrorCode::BadBytes(format!(
            "File {} is not ordered by FILE_SORTED_BY: near row {}, {}",
            self.location, row, detail,
        ))
    }
}
