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
use either::Either;
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

use super::async_buffer::BufferWriter;
use super::Location;
use super::SpillerInner;
use super::SpillsBufferPool;

pub struct Properties {
    schema: Arc<Schema>,
    writer_props: WriterPropertiesPtr,
    parquet: SchemaDescriptor,
}

impl Properties {
    pub fn new(data_schema: &DataSchema) -> errors::Result<Self> {
        let writer_props = Arc::new(WriterProperties::default());
        let schema = Arc::new(Schema::from(data_schema));
        let parquet = ArrowSchemaConverter::new()
            .with_coerce_types(writer_props.coerce_types())
            .convert(&schema)?;

        Ok(Self {
            schema,
            writer_props,
            parquet,
        })
    }

    pub fn new_encoder(&self) -> RowGroupEncoder {
        RowGroupEncoder::new(&self.writer_props, self.schema.clone(), &self.parquet)
    }
}

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
            Vec::new(),
            parquet_schema.root_schema_ptr(),
            props.clone(),
        )?;

        let mut row_group_writer = file_writer.next_row_group()?;
        for writer in writers {
            writer.close()?.append_to_row_group(&mut row_group_writer)?;
        }
        row_group_writer.close()?;

        let buf = file_writer.into_inner()?;
        let parquet_bytes = bytes::Bytes::from(buf);

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
    fn new(props: &Properties, w: W) -> Result<Self> {
        let writer = SerializedFileWriter::new(
            w,
            props.parquet.root_schema_ptr(),
            props.writer_props.clone(),
        )?;
        Ok(Self {
            schema: props.schema.clone(),
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

    pub fn num_row_group(&self) -> usize {
        self.writer.flushed_row_groups().len()
    }

    pub fn is_row_group_full(&self) -> bool {
        self.writer.flushed_row_groups().len() >= i16::MAX as _
    }

    fn into_closed_writer(self) -> errors::Result<(ParquetMetaData, SerializedFileWriter<W>)> {
        let FileWriter {
            row_groups,
            mut writer,
            ..
        } = self;
        let file_metadata = writer.finish()?;
        let tp = writer.schema_descr().root_schema_ptr();
        let schema_descr = Arc::new(SchemaDescriptor::new(tp));

        let metadata = parquet::file::metadata::FileMetaData::new(
            file_metadata.version,
            file_metadata.num_rows,
            file_metadata.created_by.clone(),
            file_metadata.key_value_metadata.clone(),
            schema_descr,
            None,
        );
        let row_groups = row_groups.into_iter().map(Arc::unwrap_or_clone).collect();
        Ok((ParquetMetaData::new(metadata, row_groups), writer))
    }
}

pub struct LocalWriter {
    dir: Arc<TempDir>,
    path: TempPath,
    file: SyncDmaFile,
    buf: DmaWriteBuf,
}

impl io::Write for LocalWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len();
        if self.buf.fast_write(buf) {
            return Ok(n);
        }

        self.buf.write_all(buf)?;
        self.buf.flush_if_full(&mut self.file)?;
        self.path.set_size(self.file.length()).unwrap();
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.buf.flush(&mut self.file)
    }
}

impl FileWriter<LocalWriter> {
    pub(super) fn finish(self) -> errors::Result<(ParquetMetaData, TempPath)> {
        let (metadata, writer) = self.into_closed_writer()?;
        let path = writer.inner().path.clone();
        Ok((metadata, path))
    }

    pub fn check_grow(&self, grow: usize, check_disk: bool) -> io::Result<bool> {
        self.writer.inner().dir.check_grow(grow, check_disk)
    }
}

impl FileWriter<BufferWriter> {
    pub(super) fn finish(self) -> errors::Result<(ParquetMetaData, usize)> {
        let (metadata, mut writer) = self.into_closed_writer()?;
        let object_meta = writer.inner_mut().finish()?;
        Ok((metadata, object_meta.content_length() as _))
    }
}

pub(super) struct FileReader {
    meta: Arc<ParquetMetaData>,
    reader: Either<AsyncDmaFile, Reader>,
}

impl AsyncFileReader for FileReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, errors::Result<bytes::Bytes>> {
        async move {
            match &mut self.reader {
                Either::Left(file) => {
                    let (dma_buf, rt_range) = file.read_range(range).await?;
                    Ok(dma_buffer_to_bytes(dma_buf).slice(rt_range))
                }
                Either::Right(reader) => Ok(reader
                    .read(range)
                    .await
                    .map_err(|err| errors::ParquetError::External(Box::new(err)))?
                    .to_bytes()),
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

pub enum AnyFileWriter {
    Local(FileWriter<LocalWriter>),
    Remote(String, FileWriter<BufferWriter>),
}

impl AnyFileWriter {
    pub(super) fn new_row_group(&self) -> RowGroupEncoder {
        match self {
            AnyFileWriter::Local(file_writer) => file_writer.new_row_group(),
            AnyFileWriter::Remote(_, file_writer) => file_writer.new_row_group(),
        }
    }
}

impl<A> SpillerInner<A> {
    pub(super) async fn new_file_writer(
        &self,
        props: &Properties,
        pool: &Arc<SpillsBufferPool>,
        chunk: usize,
        local_file_size: Option<usize>,
    ) -> Result<AnyFileWriter> {
        let op = &self.operator;

        if let (Some(dir), Some(size)) = (&self.temp_dir, local_file_size) {
            if let Some(mut path) = dir.new_file_with_size(size)? {
                path.set_size(0).unwrap();
                let file = SyncDmaFile::create(&path, true)?;
                let align = dir.block_alignment();
                let buf = DmaWriteBuf::new(align, chunk);

                let w = LocalWriter {
                    dir: dir.clone(),
                    path,
                    file,
                    buf,
                };
                return Ok(AnyFileWriter::Local(FileWriter::new(props, w)?));
            }
        };

        let remote_location = self.create_unique_location();
        let remote_writer = op.writer(&remote_location).await?;
        let remote = pool.buffer_write(remote_writer);

        Ok(AnyFileWriter::Remote(
            remote_location,
            FileWriter::new(props, remote)?,
        ))
    }

    pub(super) async fn load_row_groups(
        &self,
        location: &Location,
        meta: Arc<ParquetMetaData>,
        schema: &DataSchema,
        row_groups: Vec<usize>,
    ) -> Result<Vec<DataBlock>> {
        let op = &self.operator;
        let input = match location {
            Location::Local(path) => {
                let alignment = Some(self.temp_dir.as_ref().unwrap().block_alignment());
                let file = AsyncDmaFile::open(path, true, alignment).await?;
                FileReader {
                    meta,
                    reader: Either::Left(file),
                }
            }
            Location::Remote(path) => FileReader {
                meta,
                reader: Either::Right(op.reader(path).await?),
            },
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
    use databend_common_exception::Result;
    use databend_common_expression::types::array::ArrayColumnBuilder;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;

    use super::*;

    fn sample_block() -> DataBlock {
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

        DataBlock::new_from_columns(vec![
            StringType::from_data(vec!["alpha", "beta", "gamma"]),
            array_column,
            StringType::from_opt_data(vec![Some("nullable"), None, Some("value")]),
        ])
    }

    #[test]
    fn test_file_writer_spill_creates_metadata() -> Result<()> {
        let block = sample_block();
        let schema = block.infer_schema();
        let props = Properties::new(&schema)?;

        let mut file_writer = FileWriter::new(&props, Vec::<u8>::new())?;
        let num_rows = block.num_rows();

        let mut row_group = file_writer.new_row_group();
        row_group.add(block)?;
        let row_group = file_writer.flush_row_group(row_group)?;

        assert_eq!(row_group.num_rows() as usize, num_rows);
        assert_eq!(file_writer.num_row_group(), 1);

        Ok(())
    }

    #[test]
    fn test_row_group_writer_restores() -> Result<()> {
        let block = sample_block();
        let data_schema = block.infer_schema();

        let props = Properties::new(&data_schema)?;
        let file_writer = FileWriter::new(&props, Vec::<u8>::new())?;
        let mut row_group = file_writer.new_row_group();

        row_group.add(block.clone())?;
        row_group.add(block.clone())?;
        let restored = row_group.into_block()?;

        let expected = DataBlock::concat(&[block.clone(), block])?;
        for (exp, got) in expected.columns().iter().zip(restored.columns()) {
            assert_eq!(exp, got);
        }

        Ok(())
    }
}
