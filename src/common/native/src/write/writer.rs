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
use databend_common_expression::TableSchema;

use super::common::WriteOptions;
use super::common::write_eof;
use crate::ColumnMeta;
use crate::STRAWBOAT_MAGIC;
use crate::STRAWBOAT_VERSION;
use crate::error::Error;
use crate::error::Result;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    None,
    Started,
    Written,
    Finished,
}

/// Arrow file writer
pub struct NativeWriter<W: Write> {
    /// The object to write to
    pub(crate) writer: OffsetWriter<W>,
    /// pa write options
    pub(crate) options: WriteOptions,
    /// A reference to the schema, used in validating record batches
    pub(crate) schema: TableSchema,

    /// Record blocks that will be written as part of the strawboat footer
    pub metas: Vec<ColumnMeta>,

    pub(crate) scratch: Vec<u8>,
    /// Whether the writer footer has been written, and the writer is finished
    pub(crate) state: State,
}

impl<W: Write> NativeWriter<W> {
    /// Creates a new [`NativeWriter`] and writes the header to `writer`
    pub fn try_new(writer: W, schema: &TableSchema, options: WriteOptions) -> Result<Self> {
        let mut slf = Self::new(writer, schema.clone(), options)?;
        slf.start()?;

        Ok(slf)
    }

    /// Creates a new [`NativeWriter`].
    pub fn new(writer: W, schema: TableSchema, options: WriteOptions) -> Result<Self> {
        let num_cols = schema.fields.len();
        Ok(Self {
            writer: OffsetWriter {
                w: writer,
                offset: 0,
            },
            options,
            schema,
            metas: Vec::with_capacity(num_cols),
            scratch: Vec::with_capacity(0),
            state: State::None,
        })
    }

    /// Consumes itself into the inner writer
    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer.w
    }

    /// Writes the header and first (schema) message to the file.
    /// # Errors
    /// Errors if the file has been started or has finished.
    pub fn start(&mut self) -> Result<()> {
        if self.state != State::None {
            return Err(Error::OutOfSpec(
                "The strawboat file can only be started once".to_string(),
            ));
        }
        // write magic to header, 2 + 2 + 4 = 8 bytes
        self.writer.write_all(&STRAWBOAT_MAGIC[..])?;
        self.writer.write_all(&STRAWBOAT_VERSION.to_le_bytes())?;
        self.writer.write_all(&[0, 0, 0, 0])?;

        self.state = State::Started;
        Ok(())
    }

    /// Writes [`Chunk`] to the file
    pub fn write(&mut self, chunk: &[Column]) -> Result<()> {
        if self.state == State::Written {
            return Err(Error::OutOfSpec(
                "The strawboat file can only accept one RowGroup in a single file".to_string(),
            ));
        }
        if self.state != State::Started {
            return Err(Error::OutOfSpec(
                "The strawboat file must be started before it can be written to. Call `start` before `write`".to_string(),
            ));
        }
        assert_eq!(chunk.len(), self.schema.fields.len());
        self.encode_chunk(chunk)?;

        self.state = State::Written;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<()> {
        if self.state != State::Written {
            return Err(Error::OutOfSpec(
                "The strawboat file must be written before it can be finished. Call `start` before `finish`".to_string(),
            ));
        }
        // write footer
        // footer = schema(variable bytes) + column_meta(variable bytes)
        // + schema size(4 bytes) + column_meta size(4bytes) + EOS(8 bytes)
        let schema_bytes = serde_json::to_vec(&self.schema)?;
        // write the schema, set the written bytes to the schema
        self.writer.write_all(&schema_bytes)?;

        let meta_start = self.writer.offset();
        {
            self.writer.write_all(&self.metas.len().to_le_bytes())?;
            for meta in &self.metas {
                self.writer.write_all(&meta.offset.to_le_bytes())?;
                self.writer.write_all(&meta.pages.len().to_le_bytes())?;

                for page in meta.pages.iter() {
                    self.writer.write_all(&page.length.to_le_bytes())?;
                    self.writer.write_all(&page.num_values.to_le_bytes())?;
                }
            }
        }
        let meta_end = self.writer.offset();

        // 4 bytes for schema size
        let schema_size = schema_bytes.len();
        self.writer.write_all(&(schema_size as u32).to_le_bytes())?;
        // 4 bytes for meta_size
        self.writer
            .write_all(&((meta_end - meta_start) as u32).to_le_bytes())?;
        // write EOS
        write_eof(&mut self.writer, 0)?;
        self.writer.flush()?;
        self.state = State::Finished;
        Ok(())
    }

    /// The total size of the strawboat file in bytes
    pub fn total_size(&self) -> usize {
        self.writer.offset()
    }
}

pub struct OffsetWriter<W: Write> {
    pub w: W,
    pub offset: u64,
}

impl<W: Write> std::io::Write for OffsetWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let size = self.w.write(buf)?;
        self.offset += size as u64;
        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

pub trait OffsetWrite: std::io::Write {
    fn offset(&self) -> usize;
}

impl<W: std::io::Write> OffsetWrite for OffsetWriter<W> {
    fn offset(&self) -> usize {
        self.offset as usize
    }
}
