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

use std::io::Write;

use databend_common_expression::Column;

use super::NativeWriter;
use super::write;
use crate::ColumnMeta;
use crate::EOF_MARKER;
use crate::PageMeta;
use crate::compression::CommonCompression;
use crate::compression::Compression;
use crate::error::Result;
use crate::nested::slice_nest_column;
use crate::nested::to_leaves;
use crate::nested::to_nested;

/// Options declaring the behaviour of writing to IPC
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WriteOptions {
    /// Whether the buffers should be compressed and which codec to use.
    /// Note: to use compression the crate must be compiled with feature `io_ipc_compression`.
    pub default_compression: CommonCompression,
    /// If some encoding method performs over this ratio, we will switch to use it.
    pub default_compress_ratio: Option<f64>,
    pub max_page_size: Option<usize>,
    pub forbidden_compressions: Vec<Compression>,
}

impl<W: Write> NativeWriter<W> {
    /// Encode and write columns to the file
    pub fn encode_chunk(&mut self, chunk: &[Column]) -> Result<()> {
        assert!(!chunk.is_empty());
        let rows = chunk.first().map(|c| c.len()).unwrap();
        let page_size = self.options.max_page_size.unwrap_or(rows).min(rows);

        for column in chunk.iter() {
            let length = column.len();
            let nested = to_nested(column)?;
            let leaf_columns = to_leaves(column);
            for (leaf_column, nested) in leaf_columns.iter().zip(nested.into_iter()) {
                let leaf_column = leaf_column.clone();
                let mut page_metas = Vec::with_capacity((length + 1) / page_size + 1);
                let start = self.writer.offset;

                for offset in (0..length).step_by(page_size) {
                    let length = if offset + page_size > length {
                        length - offset
                    } else {
                        page_size
                    };

                    let mut sub_column = leaf_column.clone();
                    let mut sub_nested = nested.clone();
                    slice_nest_column(&mut sub_column, &mut sub_nested, offset, length);

                    {
                        let page_start = self.writer.offset;
                        write(
                            &mut self.writer,
                            &sub_column,
                            &sub_nested,
                            self.options.clone(),
                            &mut self.scratch,
                        )
                        .unwrap();

                        let page_end = self.writer.offset;
                        page_metas.push(PageMeta {
                            length: (page_end - page_start),
                            num_values: sub_column.len() as u64,
                        });
                    }
                }

                self.metas.push(ColumnMeta {
                    offset: start,
                    pages: page_metas,
                })
            }
        }
        Ok(())
    }
}

/// Write a record batch to the writer, writing the message size before the message
/// if the record batch is being written to a stream
pub fn write_eof<W: Write>(writer: &mut W, total_len: i32) -> Result<usize> {
    writer.write_all(&EOF_MARKER)?;
    writer.write_all(&total_len.to_le_bytes()[..])?;
    Ok(8)
}
