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

use std::io;
use std::io::Write;
use std::sync::Arc;

use arrow_schema::Schema;
use databend_common_base::base::DmaWriteBuf;
use databend_common_base::base::SyncDmaFile;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_cache::TempDir;
use databend_storages_common_cache::TempPath;
use parquet::arrow::arrow_writer::compute_leaves;
use parquet::arrow::arrow_writer::get_column_writers;
use parquet::arrow::arrow_writer::ArrowColumnWriter;
use parquet::arrow::ArrowSchemaConverter;
use parquet::errors;
use parquet::file::metadata::RowGroupMetaDataPtr;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::format::FileMetaData;
use parquet::schema::types::SchemaDescriptor;

use super::async_buffer::BufferWriter;

pub struct RowGroupWriter {
    schema: Arc<Schema>,
    writers: Vec<ArrowColumnWriter>,
}

impl RowGroupWriter {
    fn new(props: &WriterPropertiesPtr, schema: Arc<Schema>, parquet: &SchemaDescriptor) -> Self {
        let writers = get_column_writers(parquet, props, &schema).unwrap();
        Self { schema, writers }
    }

    pub(super) fn write(&mut self, block: DataBlock) -> errors::Result<()> {
        let mut writer_iter = self.writers.iter_mut();
        for (field, entry) in self.schema.fields().iter().zip(block.take_columns()) {
            let array = (&entry.to_column()).into();
            for col in compute_leaves(field, &array).unwrap() {
                writer_iter.next().unwrap().write(&col)?;
            }
        }
        Ok(())
    }

    fn close<W: Write + Send>(
        self,
        writer: &mut SerializedRowGroupWriter<'_, W>,
    ) -> errors::Result<()> {
        for w in self.writers {
            w.close()?.append_to_row_group(writer)?
        }
        Ok(())
    }

    pub fn memory_size(&self) -> usize {
        self.writers.iter().map(|w| w.memory_size()).sum()
    }
}

pub struct FileWriter<W: Write + Send> {
    schema: Arc<Schema>,
    writer: SerializedFileWriter<W>,
}

impl<W: Write + Send> FileWriter<W> {
    pub(super) fn new(
        props: Arc<WriterProperties>,
        table_schema: &TableSchema,
        w: W,
    ) -> errors::Result<Self> {
        let schema = Arc::new(Schema::from(table_schema));

        let parquet = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&schema)?;

        let writer = SerializedFileWriter::new(w, parquet.root_schema_ptr(), props.clone())?;
        Ok(Self { schema, writer })
    }

    pub(super) fn new_row_group(&self) -> RowGroupWriter {
        RowGroupWriter::new(
            self.writer.properties(),
            self.schema.clone(),
            self.writer.schema_descr(),
        )
    }

    pub(super) fn flush_row_group(
        &mut self,
        row_group: RowGroupWriter,
    ) -> errors::Result<RowGroupMetaDataPtr> {
        let mut row_group_writer = self.writer.next_row_group()?;
        row_group.close(&mut row_group_writer)?;
        row_group_writer.close()
    }

    pub(super) fn close(self) -> errors::Result<FileMetaData> {
        self.writer.close()
    }
}

struct LocalDst {
    dir: Arc<TempDir>,
    path: TempPath,
    file: Option<SyncDmaFile>,
    buf: Option<DmaWriteBuf>,
}

pub struct MixFileWriter {
    local: LocalDst,
    remote: BufferWriter,
    remote_offset: usize,
}

impl MixFileWriter {
    pub fn new(
        dir: Arc<TempDir>,
        path: TempPath,
        file: SyncDmaFile,
        buf: DmaWriteBuf,
        remote: BufferWriter,
    ) -> Self {
        MixFileWriter {
            local: LocalDst {
                dir,
                path,
                file: Some(file),
                buf: Some(buf),
            },
            remote,
            remote_offset: 0,
        }
    }

    pub fn finish(self) -> io::Result<MixFile> {
        let local_path = match self.local {
            mut local @ LocalDst {
                file: Some(_),
                buf: Some(_),
                ..
            } => {
                let dma = local.buf.as_mut().unwrap();

                let file = local.file.take().unwrap();
                let file_size = file.length() + dma.size();
                dma.flush_and_close(file)?;

                local.path.set_size(file_size).unwrap();

                return Ok(MixFile {
                    local_path: local.path,
                    remote_offset: 0,
                });
            }
            LocalDst { path, .. } => path,
        };

        Ok(MixFile {
            local_path,
            remote_offset: self.remote_offset,
        })
    }
}

impl io::Write for MixFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (dma_buf, offset) = if let local @ LocalDst {
            file: Some(_),
            buf: Some(_),
            ..
        } = &mut self.local
        {
            let n = buf.len();
            let dma = local.buf.as_mut().unwrap();
            if dma.fast_write(buf) {
                return Ok(n);
            }

            if local.dir.grow_size(&mut local.path, buf.len(), false)? {
                dma.write(buf)?;
                let file = local.file.as_mut().unwrap();
                dma.flush_full_buffer(file)?;
                local.path.set_size(file.length()).unwrap();
                return Ok(n);
            }

            let mut file = local.file.take().unwrap();
            dma.flush_full_buffer(&mut file)?;

            let file_size = file.length();
            local.path.set_size(file_size).unwrap();
            drop(file);

            (local.buf.take().unwrap().into_data(), file_size)
        } else {
            (vec![], 0)
        };

        if offset != 0 {
            self.remote_offset = offset;
        }

        for buf in dma_buf {
            self.remote.write(&buf)?;
        }
        self.remote.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.local {
            LocalDst {
                file: Some(file),
                buf: Some(dma),
                ..
            } => {
                // warning: not completely flushed, data may be lost
                dma.flush_full_buffer(file)?;
                return Ok(());
            }
            _ => (),
        }

        self.remote.flush()
    }
}

pub struct MixFile {
    local_path: TempPath,
    remote_offset: usize,
}
