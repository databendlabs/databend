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
use databend_common_base::runtime::Runtime;
use databend_common_exception::Result;
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
use parquet::schema::types::SchemaDescriptor;

use super::async_buffer::BufferPool;
use super::async_buffer::BufferWriter;
use super::SpillAdapter;
use super::SpillerInner;

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

    pub fn spill(&mut self, blocks: Vec<DataBlock>) -> Result<RowGroupMetaDataPtr> {
        let mut row_group = self.new_row_group();
        for block in blocks {
            row_group.write(block)?;
        }
        Ok(self.flush_row_group(row_group)?)
    }
}

impl FileWriter<UnionFileWriter> {
    pub(super) fn finish(self) -> errors::Result<UnionFile> {
        let writer = self.writer.into_inner()?;
        Ok(writer.finish()?)
    }
}

struct LocalDst {
    dir: Arc<TempDir>,
    path: TempPath,
    file: Option<SyncDmaFile>,
    buf: Option<DmaWriteBuf>,
}

pub struct UnionFileWriter {
    local: Option<LocalDst>,
    remote: String,
    remote_writer: BufferWriter,
    remote_offset: usize,
}

impl UnionFileWriter {
    pub fn new(
        dir: Arc<TempDir>,
        path: TempPath,
        file: SyncDmaFile,
        buf: DmaWriteBuf,
        remote: String,
        remote_writer: BufferWriter,
    ) -> Self {
        UnionFileWriter {
            local: Some(LocalDst {
                dir,
                path,
                file: Some(file),
                buf: Some(buf),
            }),
            remote,
            remote_writer,
            remote_offset: 0,
        }
    }

    pub fn without_local(remote: String, remote_writer: BufferWriter) -> Self {
        UnionFileWriter {
            local: None,
            remote,
            remote_writer,
            remote_offset: 0,
        }
    }

    pub fn finish(self) -> io::Result<UnionFile> {
        match self.local {
            Some(
                mut local @ LocalDst {
                    file: Some(_),
                    buf: Some(_),
                    ..
                },
            ) => {
                let dma = local.buf.as_mut().unwrap();

                let file = local.file.take().unwrap();
                let file_size = file.length() + dma.size();
                dma.flush_and_close(file)?;

                local.path.set_size(file_size).unwrap();

                Ok(UnionFile {
                    local_path: Some(local.path),
                    remote_path: self.remote,
                    remote_offset: None,
                })
            }
            Some(LocalDst { path, .. }) => Ok(UnionFile {
                local_path: Some(path),
                remote_path: self.remote,
                remote_offset: Some(self.remote_offset),
            }),
            None => Ok(UnionFile {
                local_path: None,
                remote_path: self.remote,
                remote_offset: Some(0),
            }),
        }
    }
}

impl io::Write for UnionFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (dma_buf, offset) = if let Some(
            local @ LocalDst {
                file: Some(_),
                buf: Some(_),
                ..
            },
        ) = &mut self.local
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
            self.remote_writer.write(&buf)?;
        }
        self.remote_writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(LocalDst {
            file: Some(file),
            buf: Some(dma),
            ..
        }) = &mut self.local
        {
            // warning: not completely flushed, data may be lost
            dma.flush_full_buffer(file)?;
            return Ok(());
        }

        self.remote_writer.flush()
    }
}

#[derive(Debug)]
pub struct UnionFile {
    local_path: Option<TempPath>,
    remote_path: String,
    remote_offset: Option<usize>,
}

impl<A: SpillAdapter> SpillerInner<A> {
    pub(super) async fn new_file_writer(
        &self,
        schema: &TableSchema,
        executor: Arc<Runtime>,
        max_buffer: usize,
    ) -> Result<FileWriter<UnionFileWriter>> {
        let pool = BufferPool::create(executor, max_buffer, 3);

        let op = self.local_operator.as_ref().unwrap_or(&self.operator);

        let remote_location = self.create_unique_location();
        let remote_writer = op.writer(&remote_location).await?;
        let remote = pool.buffer_write(remote_writer);

        let union = if let Some(disk) = &self.temp_dir {
            if let Some(path) = disk.new_file_with_size(0)? {
                let file = SyncDmaFile::create(&path, true)?;
                let align = disk.block_alignment();
                let buf = DmaWriteBuf::new(align, 4 * 1024 * 1024);
                UnionFileWriter::new(disk.clone(), path, file, buf, remote_location, remote)
            } else {
                UnionFileWriter::without_local(remote_location, remote)
            }
        } else {
            UnionFileWriter::without_local(remote_location, remote)
        };

        let props = WriterProperties::default().into();
        Ok(FileWriter::new(props, schema, union)?)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_exception::Result;
    use databend_common_expression::infer_table_schema;
    use databend_common_expression::types::UInt64Type;
    use databend_common_expression::FromData;
    use opendal::Operator;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::spillers::async_buffer::BufferPool;
    use crate::test_kits::ConfigBuilder;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_xxx() -> Result<()> {
        let config = ConfigBuilder::create().build();
        let fixture = TestFixture::setup_with_config(&config).await?;
        let _ctx = fixture.new_query_ctx().await?;

        let props = WriterProperties::default().into();

        let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![7, 8, 9])]);

        let table_schema = infer_table_schema(&block.infer_schema())?;
        let executor = GlobalIORuntime::instance();
        let memory = 1024 * 1024 * 100;

        let pool = BufferPool::create(executor, memory, 3);

        let builder = opendal::services::Fs::default().root("/tmp");
        let op = Operator::new(builder)?.finish();

        let writer = op.writer("path").await?;
        let remote = pool.buffer_write(writer);

        // let dir = todo!();
        // let path = todo!();

        // let file = SyncDmaFile::create(path, true)?;
        // let align = todo!();
        // let buf = DmaWriteBuf::new(align, 4 * 1024 * 1024);

        let file = UnionFileWriter::without_local("path".to_string(), remote);
        let mut file_writer = FileWriter::new(props, &table_schema, file)?;

        let x = file_writer.spill(vec![block])?;
        println!("{x:#?}");

        let x = file_writer.finish()?;
        println!("{x:#?}");

        Ok(())
    }
}
