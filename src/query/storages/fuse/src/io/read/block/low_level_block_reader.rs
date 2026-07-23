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

//! Low-level, logical-column-oriented FUSE block reader.

use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use opendal::Buffer;
use opendal::Operator;
use parking_lot::Mutex;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::basic::Compression as ParquetCompression;
use parquet::column::page::PageIterator;
use parquet::column::page::PageReader;
use parquet::errors::ParquetError;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::metadata::FileMetaData;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::ChunkReader;
use parquet::file::reader::Length;
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::schema::types::SchemaDescriptor;

const DEFAULT_BATCH_SIZE: usize = 8192;
const DEFAULT_WINDOW_SIZE: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_PREFETCH: usize = 2;

/// Immutable configuration for reading one FUSE block one logical column at a time.
pub struct FuseLowLevelBlockReadOptions {
    operator: Operator,
    schema: TableSchemaRef,
    block_meta: Arc<BlockMeta>,
    batch_size: usize,
    window_size: usize,
    max_prefetch: usize,
}

impl FuseLowLevelBlockReadOptions {
    pub fn new(operator: Operator, schema: TableSchemaRef, block_meta: Arc<BlockMeta>) -> Self {
        Self {
            operator,
            schema,
            block_meta,
            batch_size: DEFAULT_BATCH_SIZE,
            window_size: DEFAULT_WINDOW_SIZE,
            max_prefetch: DEFAULT_MAX_PREFETCH,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_window_size(mut self, window_size: usize) -> Self {
        self.window_size = window_size;
        self
    }

    pub fn with_max_prefetch(mut self, max_prefetch: usize) -> Self {
        self.max_prefetch = max_prefetch;
        self
    }

    fn validate(&self) -> Result<()> {
        if self.batch_size == 0 {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockReader batch_size must be greater than zero",
            ));
        }
        if self.window_size == 0 {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockReader window_size must be greater than zero",
            ));
        }
        if self.max_prefetch == 0 {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockReader max_prefetch must be greater than zero",
            ));
        }
        if self.schema.fields().is_empty() {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockReader requires a non-empty schema",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct LeafReadPlan {
    column_id: ColumnId,
    leaf_index: usize,
    range: Range<u64>,
    num_values: u64,
}

#[derive(Clone)]
struct ColumnReadPlan {
    field: TableField,
    leaves: Vec<LeafReadPlan>,
}

/// Reads a FUSE Parquet block one complete logical table column at a time.
pub struct FuseLowLevelBlockReader {
    operator: Operator,
    path: String,
    arrow_schema: Arc<Schema>,
    parquet_schema: Arc<SchemaDescriptor>,
    row_count: usize,
    compression: ParquetCompression,
    batch_size: usize,
    window_size: usize,
    max_prefetch: usize,
    columns: Vec<ColumnReadPlan>,
    next_column: usize,
}

impl FuseLowLevelBlockReader {
    pub fn create(options: FuseLowLevelBlockReadOptions) -> Result<Self> {
        options.validate()?;

        let row_count = usize::try_from(options.block_meta.row_count).map_err(|_| {
            ErrorCode::BadArguments(format!(
                "FUSE block row count {} does not fit usize",
                options.block_meta.row_count
            ))
        })?;
        let arrow_schema = Arc::new(Schema::from(options.schema.as_ref()));
        let parquet_schema = Arc::new(
            ArrowSchemaConverter::new()
                .convert(arrow_schema.as_ref())
                .map_err(ErrorCode::from)?,
        );
        let leaf_ids = options.schema.to_leaf_column_ids();
        if leaf_ids.len() != parquet_schema.num_columns() {
            return Err(ErrorCode::Internal(format!(
                "FUSE schema has {} leaf column ids but Parquet schema has {} leaves",
                leaf_ids.len(),
                parquet_schema.num_columns()
            )));
        }

        let leaf_indices = leaf_ids
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index))
            .collect::<HashMap<_, _>>();
        let mut columns = Vec::with_capacity(options.schema.num_fields());
        for field in options.schema.fields() {
            let mut leaves = Vec::with_capacity(field.data_type().num_leaf_columns());
            for column_id in field.leaf_column_ids() {
                let leaf_index = *leaf_indices.get(&column_id).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "column id {column_id} of field {} has no Parquet leaf index",
                        field.name()
                    ))
                })?;
                let meta = options
                    .block_meta
                    .col_metas
                    .get(&column_id)
                    .ok_or_else(|| {
                        ErrorCode::BadArguments(format!(
                            "FUSE block metadata has no Parquet leaf {column_id} for field {}",
                            field.name()
                        ))
                    })?;
                let (offset, len, num_values) = match meta {
                    ColumnMeta::Parquet(meta) => (meta.offset, meta.len, meta.num_values),
                };
                let end = offset.checked_add(len).ok_or_else(|| {
                    ErrorCode::BadArguments(format!(
                        "Parquet leaf {column_id} range overflows: offset {offset}, length {len}"
                    ))
                })?;
                leaves.push(LeafReadPlan {
                    column_id,
                    leaf_index,
                    range: offset..end,
                    num_values,
                });
            }
            if leaves.len() != field.data_type().num_leaf_columns() {
                return Err(ErrorCode::Internal(format!(
                    "field {} declares {} leaves but resolved {}",
                    field.name(),
                    field.data_type().num_leaf_columns(),
                    leaves.len()
                )));
            }
            columns.push(ColumnReadPlan {
                field: field.clone(),
                leaves,
            });
        }

        Ok(Self {
            operator: options.operator,
            path: options.block_meta.location.0.clone(),
            arrow_schema,
            parquet_schema,
            row_count,
            compression: options.block_meta.compression.into(),
            batch_size: options.batch_size,
            window_size: options.window_size,
            max_prefetch: options.max_prefetch,
            columns,
            next_column: 0,
        })
    }

    pub fn has_next_column(&self) -> bool {
        self.next_column < self.columns.len()
    }

    pub fn next_column(mut self) -> Result<FuseLowLevelColumnReader> {
        let plan = self.columns.get(self.next_column).cloned().ok_or_else(|| {
            ErrorCode::BadArguments("FuseLowLevelBlockReader has no remaining logical columns")
        })?;
        let row_group = StreamingRowGroup::try_create(&self, &plan)?;
        let leaf_indices = plan.leaves.iter().map(|leaf| leaf.leaf_index);
        let mask = ProjectionMask::leaves(self.parquet_schema.as_ref(), leaf_indices);
        let levels = parquet_to_arrow_field_levels(
            self.parquet_schema.as_ref(),
            mask,
            Some(self.arrow_schema.fields()),
        )?;
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &levels,
            &row_group,
            self.batch_size,
            None,
        )?;

        self.next_column += 1;
        Ok(FuseLowLevelColumnReader {
            parent: Some(self),
            field: plan.field,
            reader,
            expected_rows: row_group.num_rows,
            rows_read: 0,
            state: ColumnReaderState::Reading,
        })
    }

    pub fn finish(self) -> Result<()> {
        if self.has_next_column() {
            return Err(ErrorCode::BadArguments(format!(
                "FuseLowLevelBlockReader read {} of {} logical columns",
                self.next_column,
                self.columns.len()
            )));
        }
        Ok(())
    }
}

enum ColumnReaderState {
    Reading,
    Finished,
    Failed(String),
}

/// Active reader for one logical table column.
pub struct FuseLowLevelColumnReader {
    parent: Option<FuseLowLevelBlockReader>,
    field: TableField,
    reader: ParquetRecordBatchReader,
    expected_rows: usize,
    rows_read: usize,
    state: ColumnReaderState,
}

impl FuseLowLevelColumnReader {
    pub fn field(&self) -> &TableField {
        &self.field
    }

    pub fn rows_read(&self) -> usize {
        self.rows_read
    }

    pub fn finish(mut self) -> Result<FuseLowLevelBlockReader> {
        match &self.state {
            ColumnReaderState::Finished => Ok(self.parent.take().expect("column reader parent")),
            ColumnReaderState::Reading => Err(ErrorCode::BadArguments(format!(
                "column {} has not been read to completion",
                self.field.name()
            ))),
            ColumnReaderState::Failed(error) => Err(ErrorCode::ParquetFileInvalid(format!(
                "column {} failed while reading: {error}",
                self.field.name()
            ))),
        }
    }

    fn fail(&mut self, error: impl ToString) {
        self.state = ColumnReaderState::Failed(error.to_string());
    }
}

impl Iterator for FuseLowLevelColumnReader {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        if !matches!(self.state, ColumnReaderState::Reading) {
            return None;
        }

        match self.reader.next() {
            Some(Ok(batch)) => {
                if batch.num_columns() != 1 {
                    let error = ErrorCode::Internal(format!(
                        "column {} projection returned {} Arrow columns",
                        self.field.name(),
                        batch.num_columns()
                    ));
                    self.fail(error.message());
                    return Some(Err(error));
                }
                let rows = batch.num_rows();
                self.rows_read += rows;
                if self.rows_read > self.expected_rows {
                    let error = ErrorCode::ParquetFileInvalid(format!(
                        "column {} decoded {} rows, expected {}",
                        self.field.name(),
                        self.rows_read,
                        self.expected_rows
                    ));
                    self.fail(error.message());
                    return Some(Err(error));
                }
                let data_type = DataType::from(self.field.data_type());
                match Column::from_arrow_rs(batch.column(0).clone(), &data_type) {
                    Ok(column) => Some(Ok(column)),
                    Err(error) => {
                        self.fail(error.message());
                        Some(Err(error))
                    }
                }
            }
            Some(Err(error)) => {
                self.fail(&error);
                Some(Err(error.into()))
            }
            None => {
                if self.rows_read != self.expected_rows {
                    let error = ErrorCode::ParquetFileInvalid(format!(
                        "column {} decoded {} rows, expected {}",
                        self.field.name(),
                        self.rows_read,
                        self.expected_rows
                    ));
                    self.fail(error.message());
                    Some(Err(error))
                } else {
                    self.state = ColumnReaderState::Finished;
                    None
                }
            }
        }
    }
}

struct WindowRead {
    ranges: OperatorRangeReader,
    expected_lengths: Vec<usize>,
    next_range: usize,
    current: Option<Buffer>,
}

impl WindowRead {
    fn try_create(
        operator: Operator,
        path: String,
        range: Range<u64>,
        window_size: usize,
        max_prefetch: usize,
    ) -> Result<Self> {
        let windows = split_range(range, window_size)?;
        let expected_lengths = windows
            .iter()
            .map(|range| (range.end - range.start) as usize)
            .collect();
        let settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: window_size as u64,
            parquet_fast_read_bytes: 0,
        };
        let ranges =
            OperatorRangeReader::create(&settings, operator, path, &windows, max_prefetch)?;
        Ok(Self {
            ranges,
            expected_lengths,
            next_range: 0,
            current: None,
        })
    }

    fn load_next(&mut self) -> io::Result<bool> {
        if self.next_range == self.expected_lengths.len() {
            return Ok(false);
        }
        let data = self
            .ranges
            .read()
            .map_err(|error| io::Error::other(format!("failed to read Parquet window: {error}")))?;
        let expected = self.expected_lengths[self.next_range];
        if data.len() != expected {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Parquet window {} has {} bytes, expected {expected}",
                    self.next_range,
                    data.len()
                ),
            ));
        }
        self.next_range += 1;
        self.current = Some(data);
        Ok(true)
    }
}

impl Read for WindowRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some(current) = self.current.as_mut() {
                let read = current.read(buf)?;
                if read != 0 {
                    return Ok(read);
                }
                self.current = None;
            }
            if !self.load_next()? {
                return Ok(0);
            }
        }
    }
}

fn split_range(range: Range<u64>, window_size: usize) -> Result<Vec<Range<u64>>> {
    let window_size = u64::try_from(window_size)
        .map_err(|_| ErrorCode::BadArguments("Parquet window size does not fit u64"))?;
    let mut windows = Vec::new();
    let mut start = range.start;
    while start < range.end {
        let end = start.saturating_add(window_size).min(range.end);
        windows.push(start..end);
        start = end;
    }
    Ok(windows)
}

struct ForwardState {
    input: WindowRead,
    position: u64,
    len: u64,
}

struct ForwardChunkReader {
    state: Arc<Mutex<ForwardState>>,
}

impl ForwardChunkReader {
    fn new(input: WindowRead, len: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(ForwardState {
                input,
                position: 0,
                len,
            })),
        }
    }

    fn check_position(&self, start: u64) -> ParquetResult<()> {
        let position = self.state.lock().position;
        if start != position {
            return Err(ParquetError::General(format!(
                "forward Parquet reader requested offset {start}, current offset is {position}"
            )));
        }
        Ok(())
    }
}

impl Length for ForwardChunkReader {
    fn len(&self) -> u64 {
        self.state.lock().len
    }
}

impl ChunkReader for ForwardChunkReader {
    type T = ForwardRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        self.check_position(start)?;
        Ok(ForwardRead {
            state: self.state.clone(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<bytes::Bytes> {
        self.check_position(start)?;
        let mut read = ForwardRead {
            state: self.state.clone(),
        };
        let mut data = vec![0; length];
        read.read_exact(&mut data)?;
        Ok(data.into())
    }
}

struct ForwardRead {
    state: Arc<Mutex<ForwardState>>,
}

impl Read for ForwardRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self.state.lock();
        let remaining = state.len.saturating_sub(state.position);
        if remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let limit = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let read = state.input.read(&mut buf[..limit])?;
        state.position += read as u64;
        Ok(read)
    }
}

struct StreamingRowGroup {
    num_rows: usize,
    chunks: HashMap<usize, Arc<ForwardChunkReader>>,
    chunk_metadata: HashMap<usize, ColumnChunkMetaData>,
    metadata: ParquetMetaData,
}

impl StreamingRowGroup {
    fn try_create(reader: &FuseLowLevelBlockReader, plan: &ColumnReadPlan) -> Result<Self> {
        let mut chunks = HashMap::with_capacity(plan.leaves.len());
        let mut chunk_metadata = HashMap::with_capacity(plan.leaves.len());
        for leaf in &plan.leaves {
            let len = leaf.range.end - leaf.range.start;
            let input = WindowRead::try_create(
                reader.operator.clone(),
                reader.path.clone(),
                leaf.range.clone(),
                reader.window_size,
                reader.max_prefetch,
            )?;
            chunks.insert(
                leaf.leaf_index,
                Arc::new(ForwardChunkReader::new(input, len)),
            );
            chunk_metadata.insert(
                leaf.leaf_index,
                ColumnChunkMetaData::builder(reader.parquet_schema.column(leaf.leaf_index))
                    .set_compression(reader.compression)
                    .set_num_values(i64::try_from(leaf.num_values).map_err(|_| {
                        ErrorCode::BadArguments(format!(
                            "Parquet leaf {} num_values {} does not fit i64",
                            leaf.column_id, leaf.num_values
                        ))
                    })?)
                    .set_data_page_offset(0)
                    .set_total_compressed_size(i64::try_from(len).map_err(|_| {
                        ErrorCode::BadArguments(format!(
                            "Parquet leaf {} length {len} does not fit i64",
                            leaf.column_id
                        ))
                    })?)
                    .build()?,
            );
        }

        let columns = (0..reader.parquet_schema.num_columns())
            .map(|index| {
                chunk_metadata.get(&index).cloned().unwrap_or_else(|| {
                    ColumnChunkMetaData::builder(reader.parquet_schema.column(index))
                        .set_compression(reader.compression)
                        .set_data_page_offset(0)
                        .set_total_compressed_size(0)
                        .build()
                        .expect("empty synthetic Parquet column metadata")
                })
            })
            .collect();
        let row_group = RowGroupMetaData::builder(reader.parquet_schema.clone())
            .set_num_rows(reader.row_count as i64)
            .set_column_metadata(columns)
            .build()?;
        let metadata = ParquetMetaData::new(
            FileMetaData::new(
                0,
                reader.row_count as i64,
                None,
                None,
                reader.parquet_schema.clone(),
                None,
            ),
            vec![row_group],
        );
        Ok(Self {
            num_rows: reader.row_count,
            chunks,
            chunk_metadata,
            metadata,
        })
    }
}

impl RowGroups for StreamingRowGroup {
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn column_chunks(&self, index: usize) -> ParquetResult<Box<dyn PageIterator>> {
        let chunk = self.chunks.get(&index).ok_or_else(|| {
            ParquetError::General(format!(
                "projected Parquet leaf {index} has no range reader"
            ))
        })?;
        let metadata = self.chunk_metadata.get(&index).ok_or_else(|| {
            ParquetError::General(format!("projected Parquet leaf {index} has no metadata"))
        })?;
        let pages = SerializedPageReader::new(chunk.clone(), metadata, self.num_rows, None)?;
        Ok(Box::new(OnePageReader {
            reader: Some(Ok(Box::new(pages))),
        }))
    }

    fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
        Box::new(self.metadata.row_groups().iter())
    }

    fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }
}

struct OnePageReader {
    reader: Option<ParquetResult<Box<dyn PageReader>>>,
}

impl Iterator for OnePageReader {
    type Item = ParquetResult<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for OnePageReader {}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use std::collections::HashMap;

    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::AnyType;
    use databend_common_expression::types::ArrayColumn;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::string::StringType;
    use databend_storages_common_blocks::build_parquet_writer_properties;
    use databend_storages_common_table_meta::meta::BlockMeta;
    use databend_storages_common_table_meta::meta::ColumnMeta;
    use databend_storages_common_table_meta::meta::StatisticsOfColumns;
    use databend_storages_common_table_meta::table::TableCompression;
    use opendal::services::Memory;
    use parquet::format::DataPageHeader;
    use parquet::format::PageHeader;
    use parquet::format::PageType;
    use parquet::thrift::TCompactOutputProtocol;
    use parquet::thrift::TSerializable;

    use super::*;
    use crate::io::FuseLowLevelBlockWriteOptions;
    use crate::io::FuseLowLevelBlockWriter;
    use crate::io::FuseLowLevelStatisticsOptions;
    use crate::io::FuseLowLevelWriteContext;
    use crate::io::WriteSettings;

    fn test_data() -> (TableSchemaRef, Vec<Column>) {
        let schema = Arc::new(TableSchema::new(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new(
                "name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "numbers",
                TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::Int32))),
            ),
            TableField::new("pair", TableDataType::Tuple {
                fields_name: vec!["number".to_string(), "text".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ],
            }),
            TableField::new(
                "records",
                TableDataType::Array(Box::new(TableDataType::Tuple {
                    fields_name: vec!["number".to_string(), "text".to_string()],
                    fields_type: vec![
                        TableDataType::Number(NumberDataType::Int32),
                        TableDataType::String,
                    ],
                })),
            ),
            TableField::new(
                "lookup",
                TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![TableDataType::String, TableDataType::String],
                })),
            ),
        ]));
        let offsets: databend_common_column::buffer::Buffer<u64> =
            vec![0_u64, 2, 2, 3, 5, 6].into();
        let columns = vec![
            Int32Type::from_data(vec![10, 20, 30, 40, 50]),
            StringType::from_opt_data(vec![Some("a"), None, Some("ccc"), Some("d"), None]),
            Column::Array(Box::new(ArrayColumn::<AnyType>::new(
                Int32Type::from_data(vec![1, 2, 3, 4, 5, 6]),
                offsets.clone(),
            ))),
            Column::Tuple(vec![
                Int32Type::from_data(vec![1, 2, 3, 4, 5]),
                StringType::from_opt_data(vec![
                    Some("one"),
                    None,
                    Some("three"),
                    Some("four"),
                    None,
                ]),
            ]),
            Column::Array(Box::new(ArrayColumn::<AnyType>::new(
                Column::Tuple(vec![
                    Int32Type::from_data(vec![1, 2, 3, 4, 5, 6]),
                    StringType::from_data(vec!["a", "b", "c", "d", "e", "f"]),
                ]),
                offsets.clone(),
            ))),
            Column::Map(Box::new(ArrayColumn::<AnyType>::new(
                Column::Tuple(vec![
                    StringType::from_data(vec!["a", "b", "c", "d", "e", "f"]),
                    StringType::from_data(vec!["one", "two", "three", "four", "five", "six"]),
                ]),
                offsets,
            ))),
        ];
        (schema, columns)
    }

    fn write_options(
        operator: Operator,
        schema: TableSchemaRef,
        path: &str,
    ) -> FuseLowLevelBlockWriteOptions {
        let compression = TableCompression::Zstd;
        let properties = Arc::new(build_parquet_writer_properties(
            compression,
            true,
            None::<&StatisticsOfColumns>,
            None,
            5,
            schema.as_ref(),
            Some(2),
            Some(64),
        ));
        let context = FuseLowLevelWriteContext::new(
            FunctionContext::default(),
            operator,
            schema.clone(),
            WriteSettings {
                table_compression: compression,
                index_granularity: None,
                ..Default::default()
            },
        );
        let mut options =
            FuseLowLevelBlockWriteOptions::new(context, properties, (path.to_string(), 0));
        options.set_statistics(FuseLowLevelStatisticsOptions::new(
            schema
                .leaf_fields()
                .iter()
                .map(|field| (field.column_id(), DataType::from(field.data_type())))
                .collect(),
            Vec::new(),
            false,
        ));
        options
    }

    fn write_columns(
        operator: Operator,
        schema: TableSchemaRef,
        path: &str,
        columns: &[Column],
    ) -> BlockMeta {
        let writer =
            FuseLowLevelBlockWriter::create(write_options(operator, schema, path)).unwrap();
        let mut data = writer.write_data().unwrap();
        for source in columns {
            let mut column = data.next_column().unwrap();
            column.write(&source.slice(0..1)).unwrap();
            let split = source.len().min(4);
            column.write(&source.slice(1..split)).unwrap();
            if split < source.len() {
                column.write(&source.slice(split..source.len())).unwrap();
            }
            data = column.finish().unwrap();
        }
        data.finish().unwrap().finish().unwrap().block_meta
    }

    fn read_options(
        operator: Operator,
        schema: TableSchemaRef,
        meta: BlockMeta,
    ) -> FuseLowLevelBlockReadOptions {
        FuseLowLevelBlockReadOptions::new(operator, schema, Arc::new(meta))
            .with_batch_size(2)
            .with_window_size(1)
            .with_max_prefetch(2)
    }

    fn read_columns(
        operator: Operator,
        schema: TableSchemaRef,
        meta: BlockMeta,
    ) -> (Vec<Column>, Vec<Vec<usize>>) {
        let mut reader =
            FuseLowLevelBlockReader::create(read_options(operator, schema, meta)).unwrap();
        let mut columns = Vec::new();
        let mut boundaries = Vec::new();
        while reader.has_next_column() {
            let mut column_reader = reader.next_column().unwrap();
            let mut fragments = Vec::new();
            let mut lengths = Vec::new();
            for fragment in column_reader.by_ref() {
                let fragment = fragment.unwrap();
                lengths.push(fragment.len());
                fragments.push(fragment);
            }
            columns.push(Column::concat_columns(fragments.into_iter()).unwrap());
            boundaries.push(lengths);
            reader = column_reader.finish().unwrap();
        }
        reader.finish().unwrap();
        (columns, boundaries)
    }

    #[test]
    fn test_low_level_reader_handles_v1_record_split_across_pages() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "numbers",
            TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::Int32))),
        )]));
        let arrow_schema = Schema::from(schema.as_ref());
        let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema).unwrap();
        let descriptor = parquet_schema.column(0);
        assert!(descriptor.max_rep_level() > 0);
        let max_rep = descriptor.max_rep_level();
        let max_def = descriptor.max_def_level();

        fn encode_levels(levels: &[i16], max_level: i16) -> Vec<u8> {
            let bit_width = (16 - (max_level as u16).leading_zeros()) as usize;
            let mut packed = 0_u8;
            for (index, level) in levels.iter().enumerate() {
                packed |= (*level as u8) << (index * bit_width);
            }
            // One bit-packed group (header 3) followed by eight padded values.
            let encoded = vec![3, packed];
            let mut result = (encoded.len() as u32).to_le_bytes().to_vec();
            result.extend(encoded);
            result
        }

        fn page(levels: &[i16], max_rep: i16, max_def: i16, values: &[i32]) -> Vec<u8> {
            let mut body = encode_levels(levels, max_rep);
            body.extend(encode_levels(&vec![max_def; levels.len()], max_def));
            body.extend(values.iter().flat_map(|value| value.to_le_bytes()));
            let header = PageHeader {
                type_: PageType::DATA_PAGE,
                uncompressed_page_size: body.len() as i32,
                compressed_page_size: body.len() as i32,
                crc: None,
                data_page_header: Some(DataPageHeader {
                    num_values: levels.len() as i32,
                    encoding: parquet::format::Encoding::PLAIN,
                    definition_level_encoding: parquet::format::Encoding::RLE,
                    repetition_level_encoding: parquet::format::Encoding::RLE,
                    statistics: None,
                }),
                index_page_header: None,
                dictionary_page_header: None,
                data_page_header_v2: None,
            };
            let mut result = Vec::new();
            header
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut result))
                .unwrap();
            result.extend(body);
            result
        }

        let mut bytes = page(&[0, 1], max_rep, max_def, &[1, 2]);
        bytes.extend(page(&[1, 0, 1], max_rep, max_def, &[3, 4, 5]));
        let chunk_len = bytes.len() as u64;
        GlobalIORuntime::instance()
            .block_on(async {
                operator
                    .write("split.parquet", bytes)
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();

        let baseline = Column::Array(Box::new(ArrayColumn::<AnyType>::new(
            Int32Type::from_data(vec![1, 2, 3, 4, 5]),
            vec![0_u64, 3, 5].into(),
        )));
        let mut block_meta =
            write_columns(operator.clone(), schema.clone(), "baseline.parquet", &[
                baseline.clone(),
            ]);
        block_meta.location.0 = "split.parquet".to_string();
        block_meta.row_count = 2;
        block_meta.compression = databend_storages_common_table_meta::meta::Compression::None;
        let column_id = schema.to_leaf_column_ids()[0];
        let ColumnMeta::Parquet(mut column_meta) =
            block_meta.col_metas.get(&column_id).unwrap().clone();
        column_meta.offset = 0;
        column_meta.len = chunk_len;
        column_meta.num_values = 5;
        block_meta
            .col_metas
            .insert(column_id, ColumnMeta::Parquet(column_meta));

        let (actual, boundaries) = read_columns(operator, schema, block_meta);
        assert_eq!(actual, vec![baseline]);
        assert_eq!(boundaries, vec![vec![2]]);
    }

    #[test]
    fn test_low_level_writer_output_is_readable() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let (schema, expected) = test_data();
        let meta = write_columns(
            operator.clone(),
            schema.clone(),
            "source.parquet",
            &expected,
        );

        let (actual, boundaries) = read_columns(operator, schema, meta);
        assert_eq!(actual, expected);
        assert!(boundaries.iter().all(|lengths| lengths == &[2, 2, 1]));
    }

    #[test]
    fn test_low_level_reader_output_is_writable() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let (schema, expected) = test_data();
        let source_meta = write_columns(
            operator.clone(),
            schema.clone(),
            "source.parquet",
            &expected,
        );
        let mut reader = FuseLowLevelBlockReader::create(read_options(
            operator.clone(),
            schema.clone(),
            source_meta,
        ))
        .unwrap();
        let writer = FuseLowLevelBlockWriter::create(write_options(
            operator.clone(),
            schema.clone(),
            "copy.parquet",
        ))
        .unwrap();
        let mut data = writer.write_data().unwrap();

        while reader.has_next_column() {
            let mut input = reader.next_column().unwrap();
            let mut output = data.next_column().unwrap();
            for fragment in input.by_ref() {
                output.write(&fragment.unwrap()).unwrap();
            }
            reader = input.finish().unwrap();
            data = output.finish().unwrap();
        }
        reader.finish().unwrap();
        let copy_meta = data.finish().unwrap().finish().unwrap().block_meta;

        let (actual, _) = read_columns(operator, schema, copy_meta);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_low_level_reader_take_writer_roundtrip() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let (schema, source_columns) = test_data();
        let source_meta = write_columns(
            operator.clone(),
            schema.clone(),
            "source.parquet",
            &source_columns,
        );
        let mut reader = FuseLowLevelBlockReader::create(read_options(
            operator.clone(),
            schema.clone(),
            source_meta,
        ))
        .unwrap();
        let writer = FuseLowLevelBlockWriter::create(write_options(
            operator.clone(),
            schema.clone(),
            "selected.parquet",
        ))
        .unwrap();
        let mut data = writer.write_data().unwrap();
        let mut all_boundaries = Vec::new();

        while reader.has_next_column() {
            let mut input = reader.next_column().unwrap();
            let mut output = data.next_column().unwrap();
            let mut offset = 0;
            let mut boundaries = Vec::new();
            for fragment in input.by_ref() {
                let fragment = fragment.unwrap();
                boundaries.push(fragment.len());
                let local = (0..fragment.len())
                    .filter(|index| matches!(offset + index, 0 | 2 | 4))
                    .map(|index| index as u32)
                    .collect::<Vec<_>>();
                if !local.is_empty() {
                    let selected = DataBlock::new_from_columns(vec![fragment])
                        .take(local.as_slice())
                        .unwrap()
                        .get_by_offset(0)
                        .to_column();
                    output.write(&selected).unwrap();
                }
                offset += boundaries.last().unwrap();
            }
            all_boundaries.push(boundaries);
            reader = input.finish().unwrap();
            data = output.finish().unwrap();
        }
        reader.finish().unwrap();
        assert!(
            all_boundaries
                .iter()
                .all(|value| value == &all_boundaries[0])
        );
        let selected_meta = data.finish().unwrap().finish().unwrap().block_meta;
        assert_eq!(selected_meta.row_count, 3);

        let take_indices = [0_u32, 2, 4];
        let expected = DataBlock::new_from_columns(source_columns)
            .take(take_indices.as_slice())
            .unwrap();
        let expected = expected
            .columns()
            .iter()
            .map(|entry| entry.to_column())
            .collect::<Vec<_>>();
        let (actual, _) = read_columns(operator, schema, selected_meta);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_low_level_reader_validates_options_and_ownership() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let (schema, columns) = test_data();
        let meta = write_columns(operator.clone(), schema.clone(), "source.parquet", &columns);

        let error = FuseLowLevelBlockReader::create(
            FuseLowLevelBlockReadOptions::new(
                operator.clone(),
                schema.clone(),
                Arc::new(meta.clone()),
            )
            .with_batch_size(0),
        )
        .err()
        .unwrap();
        assert!(error.message().contains("batch_size"));
        let error = FuseLowLevelBlockReader::create(
            FuseLowLevelBlockReadOptions::new(
                operator.clone(),
                schema.clone(),
                Arc::new(meta.clone()),
            )
            .with_window_size(0),
        )
        .err()
        .unwrap();
        assert!(error.message().contains("window_size"));
        let error = FuseLowLevelBlockReader::create(
            FuseLowLevelBlockReadOptions::new(
                operator.clone(),
                schema.clone(),
                Arc::new(meta.clone()),
            )
            .with_max_prefetch(0),
        )
        .err()
        .unwrap();
        assert!(error.message().contains("max_prefetch"));

        let reader = FuseLowLevelBlockReader::create(read_options(
            operator.clone(),
            schema.clone(),
            meta.clone(),
        ))
        .unwrap();
        assert!(reader.finish().is_err());

        let reader = FuseLowLevelBlockReader::create(read_options(operator, schema, meta)).unwrap();
        let column = reader.next_column().unwrap();
        assert!(column.finish().is_err());
    }

    #[test]
    fn test_low_level_reader_rejects_missing_leaf_metadata() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let (schema, columns) = test_data();
        let mut meta = write_columns(operator.clone(), schema.clone(), "source.parquet", &columns);
        let leaf = schema.to_leaf_column_ids()[0];
        meta.col_metas.remove(&leaf);

        let error = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .err()
            .unwrap();
        assert!(error.message().contains("has no Parquet leaf"));
    }

    #[test]
    fn test_split_range_uses_fixed_windows() {
        assert_eq!(split_range(10..21, 4).unwrap(), vec![
            10..14,
            14..18,
            18..21
        ]);
        assert_eq!(split_range(10..13, 4).unwrap(), vec![10..13]);
        assert_eq!(split_range(10..10, 4).unwrap(), Vec::<Range<u64>>::new());
    }

    #[test]
    fn test_low_level_reader_reports_row_count_mismatch() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "id",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let columns = vec![Int32Type::from_data(vec![1, 2, 3, 4, 5])];
        let mut meta = write_columns(operator.clone(), schema.clone(), "source.parquet", &columns);
        meta.row_count = 6;

        let reader = FuseLowLevelBlockReader::create(read_options(operator, schema, meta)).unwrap();
        let mut column = reader.next_column().unwrap();
        let results = column.by_ref().collect::<Vec<_>>();
        assert!(results.last().unwrap().is_err());
        assert!(column.finish().is_err());
    }

    #[test]
    fn test_low_level_reader_reports_truncated_chunk() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "id",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let columns = vec![Int32Type::from_data(vec![1, 2, 3, 4, 5])];
        let mut meta = write_columns(operator.clone(), schema.clone(), "source.parquet", &columns);
        let leaf = schema.to_leaf_column_ids()[0];
        let original = meta.col_metas.get(&leaf).unwrap().clone();
        let (offset, len) = original.offset_length();
        let ColumnMeta::Parquet(mut value) = original;
        value.len = len + 1;
        value.offset = offset;
        meta.col_metas.insert(leaf, ColumnMeta::Parquet(value));

        let reader = FuseLowLevelBlockReader::create(read_options(operator, schema, meta)).unwrap();
        let mut column = reader.next_column().unwrap();
        assert!(column.any(|result| result.is_err()));
    }

    #[test]
    fn test_read_settings_do_not_merge_adjacent_windows() {
        let windows = split_range(0..17, 4).unwrap();
        let merger = databend_common_base::rangemap::RangeMerger::from_iter(windows.clone(), 0, 4);
        assert_eq!(merger.ranges(), windows);
    }

    #[test]
    fn test_leaf_ids_are_unique_in_test_schema() {
        let (schema, _) = test_data();
        let ids = schema.to_leaf_column_ids();
        assert_eq!(
            ids.len(),
            ids.iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );
        assert_eq!(
            schema
                .field_leaf_column_ids()
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
            ids
        );
        let _: HashMap<ColumnId, usize> = ids
            .into_iter()
            .enumerate()
            .map(|(index, id)| (id, index))
            .collect();
    }
}
