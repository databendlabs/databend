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

use super::write;
use super::NativeWriter;
use crate::arrow::array::*;
use crate::arrow::chunk::Chunk;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::write::num_values;
use crate::arrow::io::parquet::write::slice_parquet_array;
use crate::arrow::io::parquet::write::to_leaves;
use crate::arrow::io::parquet::write::to_nested;
use crate::arrow::io::parquet::write::to_parquet_leaves;
use crate::arrow::io::parquet::write::SchemaDescriptor;
use crate::native::compression::CommonCompression;
use crate::native::compression::Compression;
use crate::native::ColumnMeta;
use crate::native::PageMeta;
use crate::native::CONTINUATION_MARKER;

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
    /// Encode and write a [`Chunk`] to the file
    pub fn encode_chunk(
        &mut self,
        schema_descriptor: SchemaDescriptor,
        chunk: &Chunk<Box<dyn Array>>,
    ) -> Result<()> {
        let page_size = self
            .options
            .max_page_size
            .unwrap_or(chunk.len())
            .min(chunk.len());

        for (array, type_) in chunk
            .arrays()
            .iter()
            .zip(schema_descriptor.fields().to_vec())
        {
            let array = array.as_ref();
            let nested = to_nested(array, &type_)?;
            let types: Vec<parquet2::schema::types::PrimitiveType> = to_parquet_leaves(type_);
            let leaf_arrays = to_leaves(array);
            let length = array.len();

            for ((leaf_array, nested), type_) in leaf_arrays
                .iter()
                .zip(nested.into_iter())
                .zip(types.into_iter())
            {
                let start = self.writer.offset;
                let leaf_array = leaf_array.to_boxed();

                let page_metas: Vec<PageMeta> = (0..length)
                    .step_by(page_size)
                    .map(|offset| {
                        let length = if offset + page_size > length {
                            length - offset
                        } else {
                            page_size
                        };
                        let mut sub_array = leaf_array.clone();
                        let mut sub_nested = nested.clone();
                        slice_parquet_array(sub_array.as_mut(), &mut sub_nested, offset, length);
                        let page_start = self.writer.offset;
                        write(
                            &mut self.writer,
                            sub_array.as_ref(),
                            &sub_nested,
                            type_.clone(),
                            length,
                            self.options.clone(),
                            &mut self.scratch,
                        )
                        .unwrap();

                        let page_end = self.writer.offset;
                        let num_values = num_values(&sub_nested);
                        PageMeta {
                            length: (page_end - page_start),
                            num_values: num_values as u64,
                        }
                    })
                    .collect();

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
pub fn write_continuation<W: Write>(writer: &mut W, total_len: i32) -> Result<usize> {
    writer.write_all(&CONTINUATION_MARKER)?;
    writer.write_all(&total_len.to_le_bytes()[..])?;
    Ok(8)
}
