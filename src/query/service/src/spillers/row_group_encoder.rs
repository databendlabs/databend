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
use std::ops::Range;
use std::sync::Arc;

use arrow_schema::Schema;
use bytes::Bytes;
use databend_common_base::base::dma_buffer_to_bytes;
use databend_common_base::base::DmaWriteBuf;
use databend_common_base::base::SyncDmaFile;
use databend_common_base::rangemap::RangeMerger;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Value;
use databend_common_storages_parquet::parquet_reader::RowGroupCore;
use databend_common_storages_parquet::ReadSettings;
use databend_storages_common_cache::ParquetMetaData;
use databend_storages_common_cache::TempDir;
use databend_storages_common_cache::TempPath;
use either::Either;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_writer::compute_leaves;
use parquet::arrow::arrow_writer::get_column_writers;
use parquet::arrow::arrow_writer::ArrowColumnWriter;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::errors;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::metadata::RowGroupMetaDataPtr;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::schema::types::SchemaDescriptor;

use super::async_buffer::BufferWriter;
use super::async_buffer::SpillTarget;
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
        let writer_props = Arc::new(
            WriterProperties::builder()
                .set_offset_index_disabled(true)
                .set_statistics_enabled(EnabledStatistics::None)
                .build(),
        );
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
            .map(|batch| DataBlock::from_record_batch(&data_schema, &batch?))
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

struct FileReader {
    meta: Arc<ParquetMetaData>,
    reader: Either<SyncDmaFile, (Operator, String, Arc<SpillsBufferPool>)>,
    settings: ReadSettings,
    field_levels: FieldLevels,
}

impl FileReader {
    fn load_row_group(&self, i: usize) -> Result<RowGroupCore<&RowGroupMetaData>> {
        let meta = self.meta.row_group(i);
        let mut core = RowGroupCore::new(meta, None);
        core.fetch(&ProjectionMask::all(), None, |ranges| {
            self.get_ranges(ranges)
        })?;
        Ok(core)
    }

    fn get_ranges(&self, fetch_ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        match &self.reader {
            Either::Left(file) => {
                let plan = RangeFetchPlan::new(fetch_ranges, &self.settings);
                plan.read(|range| {
                    let (buffer, rt_range) = file.read_range(range.clone())?;
                    Ok(dma_buffer_to_bytes(buffer).slice(rt_range))
                })
            }
            Either::Right((op, location, pool)) => {
                pool.fetch_ranges(op.clone(), location.clone(), fetch_ranges, self.settings)
            }
        }
    }

    fn read_row_group<'a>(
        &'a self,
        schema: &DataSchema,
        row_group: &RowGroupCore<&'a RowGroupMetaData>,
        batch_size: usize,
    ) -> Result<Vec<DataBlock>> {
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            row_group,
            batch_size,
            None,
        )?;

        let mut blocks = Vec::new();
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

        Ok(blocks)
    }
}

pub struct RangeFetchPlan {
    merged: Vec<Range<u64>>,
    merged_index: Vec<(usize, Range<usize>)>,
}

impl RangeFetchPlan {
    fn new(fetch_ranges: Vec<Range<u64>>, settings: &ReadSettings) -> Self {
        let merger = RangeMerger::from_iter(
            fetch_ranges.iter().cloned(),
            settings.max_gap_size,
            settings.max_range_size,
            Some(settings.parquet_fast_read_bytes),
        );

        let merged = merger.ranges();
        let merged_index = fetch_ranges
            .into_iter()
            .map(|fetch| {
                let (index, merged) = merger
                    .get(fetch.clone())
                    .expect("range should be contained in merged ranges");
                let start = (fetch.start - merged.start) as _;
                let end = (fetch.end - merged.start) as _;
                (index, start..end)
            })
            .collect();

        Self {
            merged,
            merged_index,
        }
    }

    fn read<F>(self, load_data: F) -> Result<Vec<Bytes>>
    where F: FnMut(Range<u64>) -> Result<Bytes> {
        let merged_chunks = self
            .merged
            .into_iter()
            .map(load_data)
            .collect::<Result<Vec<_>>>()?;

        let chunks = self
            .merged_index
            .into_iter()
            .map(|(i, range)| merged_chunks[i].slice(range))
            .collect();

        Ok(chunks)
    }
}

pub enum AnyFileWriter {
    Local {
        path: TempPath,
        writer: FileWriter<LocalWriter>,
    },
    Remote {
        path: String,
        writer: FileWriter<BufferWriter>,
    },
}

impl AnyFileWriter {
    pub(super) fn new_row_group(&self) -> RowGroupEncoder {
        match self {
            AnyFileWriter::Local { writer, .. } => writer.new_row_group(),
            AnyFileWriter::Remote { writer, .. } => writer.new_row_group(),
        }
    }
}

impl<A> SpillerInner<A> {
    pub(super) fn new_file_writer(
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
                let writer = FileWriter::new(props, w)?;
                let path = writer.writer.inner().path.clone();
                return Ok(AnyFileWriter::Local { path, writer });
            }
        };

        let remote_location = self.create_unique_location();
        let remote =
            pool.buffer_writer(op.clone(), remote_location.clone(), SpillTarget::Remote)?;

        Ok(AnyFileWriter::Remote {
            path: remote_location.clone(),
            writer: FileWriter::new(props, remote)?,
        })
    }

    pub(super) fn load_row_groups(
        &self,
        location: &Location,
        meta: Arc<ParquetMetaData>,
        schema: &DataSchema,
        row_groups: Vec<usize>,
        pool: Arc<SpillsBufferPool>,
        settings: ReadSettings,
        batch_size: usize,
    ) -> Result<Vec<DataBlock>> {
        let field_levels = parquet_to_arrow_field_levels(
            meta.file_metadata().schema_descr(),
            ProjectionMask::all(),
            None,
        )?;

        let input = match (location, &self.local_operator) {
            (Location::Local(path), None) => {
                let alignment = Some(self.temp_dir.as_ref().unwrap().block_alignment());
                let file = SyncDmaFile::open(path, true, alignment)?;
                FileReader {
                    meta,
                    reader: Either::Left(file),
                    settings,
                    field_levels,
                }
            }
            (Location::Local(path), Some(local)) => FileReader {
                meta,
                reader: Either::Right((
                    local.clone(),
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    pool,
                )),
                settings,
                field_levels,
            },
            (Location::Remote(path), _) => FileReader {
                meta,
                reader: Either::Right((self.operator.clone(), path.clone(), pool)),
                settings,
                field_levels,
            },
        };

        let mut blocks = Vec::new();
        for i in row_groups {
            let row_group = input.load_row_group(i)?;
            blocks.append(&mut input.read_row_group(schema, &row_group, batch_size)?);
        }

        Ok(blocks)
    }
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

    #[test]
    fn test_range_fetch_plan_merges_ranges() -> Result<()> {
        let data: Vec<u8> = (0u8..64).collect();

        let settings = ReadSettings {
            max_gap_size: 4,
            max_range_size: 32,
            parquet_fast_read_bytes: 1,
            enable_cache: false,
        };

        let fetch_ranges = vec![5..8, 9..12, 20..24];
        let plan = RangeFetchPlan::new(fetch_ranges.clone(), &settings);
        assert_eq!(&plan.merged, &[5..12, 20..24]);

        let chunks = plan.read(|range| {
            let start = range.start as usize;
            let end = range.end as usize;
            Ok(Bytes::copy_from_slice(&data[start..end]))
        })?;

        for (range, got) in fetch_ranges.into_iter().zip(chunks) {
            assert_eq!(
                got,
                Bytes::copy_from_slice(&data[(range.start as _)..(range.end as _)])
            );
        }

        Ok(())
    }
}
