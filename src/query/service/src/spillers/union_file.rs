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

use std::future;
use std::io;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;

use arrow_schema::Schema;
use databend_common_base::base::dma_buffer_to_bytes;
use databend_common_base::base::AsyncDmaFile;
use databend_common_base::base::DmaWriteBuf;
use databend_common_base::base::SyncDmaFile;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Value;
use databend_storages_common_cache::ParquetMetaData;
use databend_storages_common_cache::TempDir;
use databend_storages_common_cache::TempPath;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use opendal::Reader;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_writer::compute_leaves;
use parquet::arrow::arrow_writer::get_column_writers;
use parquet::arrow::arrow_writer::ArrowColumnWriter;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
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

pub struct RowGroupEncoder {
    schema: Arc<Schema>,
    props: WriterPropertiesPtr,
    writers: Vec<ArrowColumnWriter>,
}

impl RowGroupEncoder {
    fn new(props: &WriterPropertiesPtr, schema: Arc<Schema>, parquet: &SchemaDescriptor) -> Self {
        let writers = get_column_writers(parquet, props, &schema).unwrap();
        Self {
            schema,
            props: props.clone(),
            writers,
        }
    }

    pub fn add(&mut self, block: DataBlock) -> errors::Result<()> {
        let columns = block.take_columns();
        let mut writer_iter = self.writers.iter_mut();
        for (field, entry) in self.schema.fields().iter().zip(columns) {
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

    pub fn into_block(self) -> Result<DataBlock> {
        let RowGroupEncoder {
            schema,
            props,
            writers,
        } = self;

        let data_schema = DataSchema::try_from(schema.as_ref())?;
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&schema)?;

        let mut file_writer = SerializedFileWriter::new(
            // todo: find a nocopy way
            Cursor::new(Vec::new()),
            parquet_schema.root_schema_ptr(),
            props.clone(),
        )?;

        let mut row_group_writer = file_writer.next_row_group()?;
        for writer in writers {
            writer.close()?.append_to_row_group(&mut row_group_writer)?;
        }
        row_group_writer.close()?;

        let cursor = file_writer.into_inner()?;
        let parquet_bytes = bytes::Bytes::from(cursor.into_inner());

        let reader = ParquetRecordBatchReader::try_new(parquet_bytes, usize::MAX)?;
        let blocks = reader
            .map(|batch| Ok(DataBlock::from_record_batch(&data_schema, &batch?)?.0))
            .collect::<Result<Vec<_>>>()?;

        if blocks.is_empty() {
            return Ok(DataBlock::empty_with_schema(Arc::new(data_schema)));
        }

        let block = if blocks.len() == 1 {
            blocks.into_iter().next().unwrap()
        } else {
            DataBlock::concat(&blocks)?
        };

        Ok(block)
    }
}

pub struct FileWriter<W: Write + Send> {
    schema: Arc<Schema>,
    row_groups: Vec<RowGroupMetaDataPtr>,
    writer: SerializedFileWriter<W>,
}

impl<W: Write + Send> FileWriter<W> {
    fn new(props: Arc<WriterProperties>, data_schema: &DataSchema, w: W) -> errors::Result<Self> {
        let schema = Arc::new(Schema::from(data_schema));

        let parquet = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&schema)?;

        let writer = SerializedFileWriter::new(w, parquet.root_schema_ptr(), props.clone())?;
        Ok(Self {
            schema,
            writer,
            row_groups: vec![],
        })
    }

    pub(super) fn new_row_group(&self) -> RowGroupEncoder {
        RowGroupEncoder::new(
            self.writer.properties(),
            self.schema.clone(),
            self.writer.schema_descr(),
        )
    }

    pub(super) fn flush_row_group(
        &mut self,
        row_group: RowGroupEncoder,
    ) -> errors::Result<RowGroupMetaDataPtr> {
        let mut row_group_writer = self.writer.next_row_group()?;
        row_group.close(&mut row_group_writer)?;
        let meta = row_group_writer.close()?;
        self.row_groups.push(meta.clone());
        Ok(meta)
    }

    pub fn spill(&mut self, blocks: Vec<DataBlock>) -> Result<RowGroupMetaDataPtr> {
        let mut row_group = self.new_row_group();
        for block in blocks {
            row_group.add(block)?;
        }

        Ok(self.flush_row_group(row_group)?)
    }
}

impl FileWriter<UnionFileWriter> {
    pub(super) fn finish(mut self) -> errors::Result<(ParquetMetaData, UnionFile)> {
        let file_metadata = self.writer.finish()?;
        let tp = self.writer.schema_descr().root_schema_ptr();
        let schema_descr = Arc::new(SchemaDescriptor::new(tp));

        let metadata = parquet::file::metadata::FileMetaData::new(
            file_metadata.version,
            file_metadata.num_rows,
            file_metadata.created_by.clone(),
            file_metadata.key_value_metadata.clone(),
            schema_descr,
            None,
        );
        let file = self.writer.inner_mut().finish()?;
        let row_groups = std::mem::take(&mut self.row_groups);
        drop(self);
        let row_groups = row_groups.into_iter().map(Arc::unwrap_or_clone).collect();
        Ok((ParquetMetaData::new(metadata, row_groups), file))
    }

    pub fn has_opening_local(&self) -> bool {
        self.writer.inner().has_opening_local()
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
    remote_writer: Option<BufferWriter>,
    remote_offset: u64,
}

impl UnionFileWriter {
    fn new(
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
            remote_writer: Some(remote_writer),
            remote_offset: 0,
        }
    }

    fn without_local(remote: String, remote_writer: BufferWriter) -> Self {
        UnionFileWriter {
            local: None,
            remote,
            remote_writer: Some(remote_writer),
            remote_offset: 0,
        }
    }

    fn finish(&mut self) -> io::Result<UnionFile> {
        let remote_size = self.remote_writer.take().unwrap().close()?.content_length();
        match self.local.take() {
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
                    remote_path: std::mem::take(&mut self.remote),
                    remote_offset: None,
                    remote_size,
                })
            }
            Some(LocalDst { path, .. }) => Ok(UnionFile {
                local_path: Some(path),
                remote_path: std::mem::take(&mut self.remote),
                remote_offset: Some(self.remote_offset),
                remote_size,
            }),
            None => Ok(UnionFile {
                local_path: None,
                remote_path: std::mem::take(&mut self.remote),
                remote_offset: Some(0),
                remote_size,
            }),
        }
    }

    pub fn has_opening_local(&self) -> bool {
        self.local
            .as_ref()
            .map(|local| local.file.is_some())
            .unwrap_or(false)
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
            self.remote_offset = offset as _;
        }

        for buf in dma_buf {
            self.remote_writer.as_mut().unwrap().write(&buf)?;
        }
        self.remote_writer.as_mut().unwrap().write(buf)
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

        self.remote_writer.as_mut().unwrap().flush()
    }
}

#[derive(Debug, Clone)]
pub struct UnionFile {
    pub local_path: Option<TempPath>,
    pub remote_path: String,
    pub remote_offset: Option<u64>,
    pub remote_size: u64,
}

pub(super) struct FileReader {
    meta: Arc<ParquetMetaData>,
    local: Option<(TempPath, AsyncDmaFile)>,
    remote_reader: Reader,
    remote_offset: Option<u64>,
}

impl AsyncFileReader for FileReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, errors::Result<bytes::Bytes>> {
        async move {
            let local_bytes = if let Some((_, file)) = &mut self.local {
                let local_range = self
                    .remote_offset
                    .map(|offset| {
                        if range.end <= offset {
                            return range.clone();
                        }
                        if range.start < offset {
                            range.start..offset
                        } else {
                            offset..offset
                        }
                    })
                    .unwrap_or(range.clone());

                let (dma_buf, rt_range) = file.read_range(local_range.clone()).await?;
                let bytes = dma_buffer_to_bytes(dma_buf).slice(rt_range);
                if local_range == range {
                    return Ok(bytes);
                }
                Some(bytes)
            } else {
                None
            };

            let remote_range = self
                .remote_offset
                .map(|offset| (range.start - offset)..(range.end - offset))
                .unwrap_or(range);

            let remote_bytes = self
                .remote_reader
                .read(remote_range)
                .await
                .map_err(|err| errors::ParquetError::External(Box::new(err)))?;

            if local_bytes.is_some() {
                Ok(
                    opendal::Buffer::from_iter(local_bytes.into_iter().chain(remote_bytes))
                        .to_bytes(),
                )
            } else {
                Ok(remote_bytes.to_bytes())
            }
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, errors::Result<Arc<ParquetMetaData>>> {
        future::ready(Ok(self.meta.clone())).boxed()
    }
}

impl<A: SpillAdapter> SpillerInner<A> {
    pub(super) async fn new_file_writer(
        &self,
        schema: &DataSchema,
        pool: &Arc<BufferPool>,
        dio: bool,
        chunk: usize,
    ) -> Result<FileWriter<UnionFileWriter>> {
        let op = self.local_operator.as_ref().unwrap_or(&self.operator);

        let remote_location = self.create_unique_location();
        let remote_writer = op.writer(&remote_location).await?;
        let remote = pool.buffer_write(remote_writer);

        let union = if let Some(disk) = &self.temp_dir {
            if let Some(path) = disk.new_file_with_size(0)? {
                let file = SyncDmaFile::create(&path, dio)?;
                let align = disk.block_alignment();
                let buf = DmaWriteBuf::new(align, chunk);
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

    pub(super) async fn load_row_groups(
        &self,
        UnionFile {
            local_path,
            remote_path,
            remote_offset,
            ..
        }: UnionFile,
        meta: Arc<ParquetMetaData>,
        schema: &DataSchema,
        row_groups: Vec<usize>,
        dio: bool,
    ) -> Result<Vec<DataBlock>> {
        let op = self.local_operator.as_ref().unwrap_or(&self.operator);

        let input = FileReader {
            meta,
            local: if let Some(path) = local_path {
                let alignment = Some(self.temp_dir.as_ref().unwrap().block_alignment());
                let file = AsyncDmaFile::open(&path, dio, alignment).await?;
                Some((path, file))
            } else {
                None
            },
            remote_offset,
            remote_reader: op.reader(&remote_path).await?,
        };

        let builder = ArrowReaderBuilder::new(input).await?;
        let stream = builder
            .with_row_groups(row_groups)
            .with_batch_size(usize::MAX)
            .build()?;

        load_blocks_from_stream(schema, stream).await
    }
}

async fn load_blocks_from_stream<T>(
    schema: &DataSchema,
    mut stream: ParquetRecordBatchStream<T>,
) -> Result<Vec<DataBlock>>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    let mut blocks = Vec::new();
    while let Some(reader) = stream.next_row_group().await? {
        for record in reader {
            let record = record?;
            let num_rows = record.num_rows();
            let mut columns = Vec::with_capacity(record.num_columns());
            for (array, field) in record.columns().iter().zip(schema.fields()) {
                let data_type = field.data_type();
                columns.push(BlockEntry::new(
                    Value::from_arrow_rs(array.clone(), data_type)?,
                    || (data_type.clone(), num_rows),
                ))
            }
            let block = DataBlock::new(columns, num_rows);
            blocks.push(block);
        }
    }

    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_exception::Result;
    use databend_common_expression::types::array::ArrayColumnBuilder;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_storage::DataOperator;
    use parquet::file::properties::WriterProperties;
    use parquet::file::properties::WriterPropertiesPtr;

    use super::*;
    use crate::spillers::async_buffer::BufferPool;
    use crate::test_kits::ConfigBuilder;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_xxx() -> Result<()> {
        let config = ConfigBuilder::create().off_log().build();
        let fixture = TestFixture::setup_with_config(&config).await?;
        let _ctx = fixture.new_query_ctx().await?;

        let props = WriterProperties::default().into();

        let block = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![7, 8, 9]),
            StringType::from_data(vec!["c", "d", "e"]),
        ]);

        let data_schema = block.infer_schema();
        let executor = GlobalIORuntime::instance();
        let memory = 1024 * 1024 * 100;

        let pool = BufferPool::create(executor, memory, 3);
        let op = DataOperator::instance().operator();

        let path = "path";
        let writer = op.writer(path).await?;
        let remote = pool.buffer_write(writer);

        // let dir = todo!();
        // let path = todo!();

        // let file = SyncDmaFile::create(path, true)?;
        // let align = todo!();
        // let buf = DmaWriteBuf::new(align, 4 * 1024 * 1024);

        let file = UnionFileWriter::without_local(path.to_string(), remote);
        let mut file_writer = FileWriter::new(props, &data_schema, file)?;

        let mut row_groups = vec![];
        let row_group = file_writer.spill(vec![block])?;
        row_groups.push((*row_group).clone());

        let (metadata, file) = file_writer.finish()?;

        let input = FileReader {
            meta: metadata.into(),
            local: None,
            remote_reader: op.reader(&file.remote_path).await?,
            remote_offset: None,
        };

        let builder = ArrowReaderBuilder::new(input).await?;
        let stream = builder.with_batch_size(usize::MAX).build()?;

        let blocks = load_blocks_from_stream(&data_schema, stream).await?;
        println!("{:?}", blocks);

        Ok(())
    }

    #[test]
    fn test_row_group_writer_restores() -> Result<()> {
        let mut array_builder = ArrayColumnBuilder::<Int32Type>::with_capacity(3, 3, &[]);
        {
            let mut arrays = array_builder.as_mut();
            arrays.put_item(1);
            arrays.put_item(2);
            arrays.commit_row();

            arrays.put_item(3);
            arrays.commit_row();

            arrays.push_default();
        }
        let array_column = Column::Array(Box::new(
            array_builder
                .build()
                .upcast(&DataType::Array(Int32Type::data_type().into())),
        ));

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(vec!["alpha", "beta", "gamma"]),
            array_column,
            StringType::from_opt_data(vec![Some("nullable"), None, Some("value")]),
        ]);

        let data_schema = block.infer_schema();

        let props: WriterPropertiesPtr = WriterProperties::default().into();
        let file_writer = FileWriter::new(props.clone(), &data_schema, Vec::<u8>::new())?;
        let mut row_group = file_writer.new_row_group();

        row_group.add(block.clone())?;
        row_group.add(block.clone())?;
        let restored = row_group.into_block()?;

        for (a, b) in DataBlock::concat(&[block.clone(), block])?
            .columns()
            .iter()
            .zip(restored.columns())
        {
            assert_eq!(a, b);
        }

        Ok(())
    }
}
