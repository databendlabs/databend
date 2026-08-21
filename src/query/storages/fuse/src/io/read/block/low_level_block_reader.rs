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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

use arrow::buffer::OffsetBuffer;
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::FixedSizeListArray;
use arrow_array::LargeListArray;
use arrow_array::ListArray;
use arrow_array::MapArray;
use arrow_array::StructArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_catalog::plan::block_id_from_location;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COLUMN_ID;
use databend_common_expression::ORIGIN_VERSION_COLUMN_ID;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_io::ChunkedRangeReader;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use opendal::Operator;
use parking_lot::Mutex;
use parquet::arrow::ArrayReader;
use parquet::arrow::ArrayReaderBuilder;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
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

use crate::io::create_file_range_reader;

const DEFAULT_WINDOW_SIZE: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_PREFETCH: usize = 2;

pub struct FuseLowLevelBlockReadOptions {
    operator: Operator,
    schema: TableSchemaRef,
    block_meta: Arc<BlockMeta>,
    default_values: Option<Vec<Scalar>>,
    stream_table_version: Option<u64>,
    cluster_key_exprs: Vec<Expr<usize>>,
    cluster_key_fields: BTreeSet<usize>,
    cluster_key_func_ctx: Option<FunctionContext>,
    window_size: usize,
    max_prefetch: usize,
    populate_cache: bool,
}

impl FuseLowLevelBlockReadOptions {
    pub fn new(operator: Operator, schema: TableSchemaRef, block_meta: Arc<BlockMeta>) -> Self {
        Self {
            operator,
            schema,
            block_meta,
            default_values: None,
            stream_table_version: None,
            cluster_key_exprs: Vec::new(),
            cluster_key_fields: BTreeSet::new(),
            cluster_key_func_ctx: None,
            window_size: DEFAULT_WINDOW_SIZE,
            max_prefetch: DEFAULT_MAX_PREFETCH,
            populate_cache: true,
        }
    }

    pub fn with_default_values(mut self, default_values: Vec<Scalar>) -> Self {
        self.default_values = Some(default_values);
        self
    }

    pub fn with_stream_table_version(mut self, table_version: u64) -> Self {
        self.stream_table_version = Some(table_version);
        self
    }

    pub fn with_cluster_keys(mut self, exprs: Vec<Expr<usize>>, func_ctx: FunctionContext) -> Self {
        self.cluster_key_fields = exprs
            .iter()
            .flat_map(|expr| expr.column_refs().into_keys())
            .collect();
        self.cluster_key_exprs = exprs;
        self.cluster_key_func_ctx = Some(func_ctx);
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

    /// Whether fetched chunks are admitted into the shared disk cache; reads
    /// still serve existing entries when disabled.
    pub fn with_populate_cache(mut self, populate_cache: bool) -> Self {
        self.populate_cache = populate_cache;
        self
    }

    fn validate(&self) -> Result<()> {
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
        if let Some(default_values) = &self.default_values
            && default_values.len() != self.schema.num_fields()
        {
            return Err(ErrorCode::BadArguments(format!(
                "FuseLowLevelBlockReader has {} default values for {} data fields",
                default_values.len(),
                self.schema.num_fields()
            )));
        }
        if self.cluster_key_func_ctx.is_some() {
            if self.cluster_key_exprs.is_empty() {
                return Err(ErrorCode::BadArguments(
                    "FuseLowLevelBlockReader requires at least one cluster-key expression",
                ));
            }
            if let Some(field) = self
                .cluster_key_fields
                .iter()
                .find(|&&field| field >= self.schema.num_fields())
            {
                return Err(ErrorCode::BadArguments(format!(
                    "cluster-key field {field} is outside the {}-field schema",
                    self.schema.num_fields()
                )));
            }
        }
        Ok(())
    }
}

pub struct FuseLowLevelBlockReader {
    operator: Operator,
    path: String,
    schema: TableSchemaRef,
    block_meta: Arc<BlockMeta>,
    default_values: Option<Vec<Scalar>>,
    stream_table_version: Option<u64>,
    cluster_key_exprs: Vec<Expr<usize>>,
    cluster_key_fields: BTreeSet<usize>,
    cluster_key_func_ctx: Option<FunctionContext>,
    arrow_schema: Arc<Schema>,
    parquet_schema: Arc<SchemaDescriptor>,
    arrow_fields: Vec<Arc<Field>>,
    leaf_columns: Vec<Vec<(ColumnId, usize)>>,
    parquet_metadata: Arc<ParquetMetaData>,
    row_count: usize,
    compression: ParquetCompression,
    window_size: usize,
    max_prefetch: usize,
    populate_cache: bool,
}

impl FuseLowLevelBlockReader {
    pub fn create(options: FuseLowLevelBlockReadOptions) -> Result<Self> {
        options.validate()?;
        let Ok(row_count) = usize::try_from(options.block_meta.row_count) else {
            return Err(ErrorCode::BadArguments(format!(
                "FUSE block row count {} does not fit usize",
                options.block_meta.row_count
            )));
        };
        let Ok(row_count_i64) = i64::try_from(options.block_meta.row_count) else {
            return Err(ErrorCode::BadArguments(format!(
                "FUSE block row count {} does not fit i64",
                options.block_meta.row_count
            )));
        };
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

        let mut leaf_indices = HashMap::with_capacity(leaf_ids.len());
        for (index, column_id) in leaf_ids.into_iter().enumerate() {
            leaf_indices.insert(column_id, index);
        }
        let mut arrow_fields = Vec::with_capacity(options.schema.num_fields());
        let mut leaf_columns = Vec::with_capacity(options.schema.num_fields());
        for (field_index, field) in options.schema.fields().iter().enumerate() {
            let column_ids = field.leaf_column_ids();
            let mut leaves = Vec::with_capacity(column_ids.len());
            for column_id in column_ids {
                let Some(&leaf_index) = leaf_indices.get(&column_id) else {
                    return Err(ErrorCode::Internal(format!(
                        "column id {column_id} of field {} has no Parquet leaf index",
                        field.name()
                    )));
                };
                leaves.push((column_id, leaf_index));
            }
            arrow_fields.push(arrow_schema.fields()[field_index].clone());
            leaf_columns.push(leaves);
        }
        let compression = options.block_meta.compression.into();
        let mut columns = Vec::with_capacity(parquet_schema.num_columns());
        for index in 0..parquet_schema.num_columns() {
            let descriptor = parquet_schema.column(index);
            let metadata = ColumnChunkMetaData::builder(descriptor)
                .set_compression(compression)
                .set_data_page_offset(0)
                .set_total_compressed_size(0)
                .build()?;
            columns.push(metadata);
        }
        let row_group = RowGroupMetaData::builder(parquet_schema.clone())
            .set_num_rows(row_count_i64)
            .set_column_metadata(columns)
            .build()?;
        let parquet_metadata = Arc::new(ParquetMetaData::new(
            FileMetaData::new(0, row_count_i64, None, None, parquet_schema.clone(), None),
            vec![row_group],
        ));

        Ok(Self {
            operator: options.operator,
            path: options.block_meta.location.0.clone(),
            schema: options.schema,
            block_meta: options.block_meta.clone(),
            default_values: options.default_values,
            stream_table_version: options.stream_table_version,
            cluster_key_exprs: options.cluster_key_exprs,
            cluster_key_fields: options.cluster_key_fields,
            cluster_key_func_ctx: options.cluster_key_func_ctx,
            arrow_schema,
            parquet_schema,
            arrow_fields,
            leaf_columns,
            parquet_metadata,
            row_count,
            compression,
            window_size: options.window_size,
            max_prefetch: options.max_prefetch,
            populate_cache: options.populate_cache,
        })
    }

    pub fn retained_window_bytes(physical_leaves: usize) -> usize {
        let retained_per_leaf =
            DEFAULT_WINDOW_SIZE.saturating_mul(DEFAULT_MAX_PREFETCH.saturating_add(2));
        retained_per_leaf.saturating_mul(physical_leaves)
    }

    pub fn read_cluster_keys(mut self) -> Result<FuseLowLevelClusterKeyReader> {
        let Some(func_ctx) = self.cluster_key_func_ctx.take() else {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockReader has no cluster-key configuration",
            ));
        };
        let fields = std::mem::take(&mut self.cluster_key_fields);
        let exprs = std::mem::take(&mut self.cluster_key_exprs);
        let mut key_readers = Vec::with_capacity(fields.len());
        for &field_index in &fields {
            key_readers.push((field_index, self.create_column_batch_reader(field_index)?));
        }
        Ok(FuseLowLevelClusterKeyReader {
            block: self,
            exprs,
            fields,
            func_ctx,
            key_readers,
            payload_readers: HashMap::new(),
        })
    }

    pub fn read_data(self) -> FuseLowLevelDataReader {
        FuseLowLevelDataReader {
            block: self,
            next_field: 0,
        }
    }

    pub fn read_column(&self, field_index: usize) -> Result<FuseLowLevelColumnBatchReader> {
        self.create_column_batch_reader(field_index)
    }

    fn create_column_batch_reader(
        &self,
        field_index: usize,
    ) -> Result<FuseLowLevelColumnBatchReader> {
        if field_index >= self.schema.num_fields() {
            return Err(ErrorCode::BadArguments(format!(
                "field index {field_index} is outside the {}-field schema",
                self.schema.num_fields()
            )));
        }
        let schema = Arc::new(self.schema.project(&[field_index]));
        let mut options = FuseLowLevelBlockReadOptions::new(
            self.operator.clone(),
            schema,
            self.block_meta.clone(),
        )
        .with_window_size(self.window_size)
        .with_max_prefetch(self.max_prefetch);
        if let Some(default_values) = &self.default_values {
            options = options.with_default_values(vec![default_values[field_index].clone()]);
        }
        if let Some(table_version) = self.stream_table_version {
            options = options.with_stream_table_version(table_version);
        }
        let reader = FuseLowLevelBlockReader::create(options)?
            .read_data()
            .next_column()?;
        Ok(FuseLowLevelColumnBatchReader::new(reader))
    }
}

/// Owns the block read context and advances through logical table columns.
pub struct FuseLowLevelDataReader {
    block: FuseLowLevelBlockReader,
    next_field: usize,
}

impl FuseLowLevelDataReader {
    pub fn has_next_column(&self) -> bool {
        self.next_field < self.block.schema.num_fields()
    }

    pub fn next_column(mut self) -> Result<FuseLowLevelColumnReader> {
        if !self.has_next_column() {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelDataReader has no remaining logical columns",
            ));
        }
        let block = &self.block;
        let field_index = self.next_field;
        let field = block.schema.field(field_index).clone();
        let arrow_field = &block.arrow_fields[field_index];
        let leaf_columns = &block.leaf_columns[field_index];
        let mut present = 0;
        for (column_id, _) in leaf_columns {
            if block.block_meta.col_metas.contains_key(column_id) {
                present += 1;
            }
        }
        if present != 0 && present != leaf_columns.len() {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "field {} has {present} of {} Parquet leaves in block {}",
                field.name(),
                leaf_columns.len(),
                block.path
            )));
        }

        let physical = if present == 0 {
            None
        } else {
            let mut leaves = Vec::with_capacity(leaf_columns.len());
            for &(column_id, leaf_index) in leaf_columns {
                let meta = block
                    .block_meta
                    .col_metas
                    .get(&column_id)
                    .expect("field presence validated");
                let (offset, len, num_values) = match meta {
                    ColumnMeta::Parquet(meta) => (meta.offset, meta.len, meta.num_values),
                };
                let Some(end) = offset.checked_add(len) else {
                    return Err(ErrorCode::BadArguments(format!(
                        "Parquet leaf {column_id} range overflows: offset {offset}, length {len}"
                    )));
                };
                leaves.push(FuseLowLevelLeafReader::create(
                    block,
                    column_id,
                    leaf_index,
                    offset..end,
                    num_values,
                )?);
            }
            let current = vec![None; leaves.len()];
            Some(PhysicalColumnReader {
                arrow_field: arrow_field.clone(),
                leaves,
                current,
            })
        };

        let source = if is_origin_column(field.column_id()) {
            let Some(table_version) = block.stream_table_version else {
                return Err(ErrorCode::BadArguments(format!(
                    "stream origin field {} requires a table version",
                    field.name()
                )));
            };
            FuseLowLevelColumnSource::Stream(StreamColumnReader {
                column_id: field.column_id(),
                source_location: block.path.clone(),
                table_version,
                physical,
            })
        } else if let Some(physical) = physical {
            FuseLowLevelColumnSource::Physical(physical)
        } else {
            let Some(default_values) = &block.default_values else {
                return Err(ErrorCode::BadArguments(format!(
                    "missing Parquet field {} has no default value",
                    field.name()
                )));
            };
            let Some(scalar) = default_values.get(field_index) else {
                return Err(ErrorCode::BadArguments(format!(
                    "missing Parquet field {} has no default value",
                    field.name()
                )));
            };
            FuseLowLevelColumnSource::Default {
                scalar: scalar.clone(),
                data_type: DataType::from(field.data_type()),
            }
        };

        let expected_rows = block.row_count;
        self.next_field += 1;
        Ok(FuseLowLevelColumnReader {
            data: self,
            field,
            source,
            expected_rows,
            rows_returned: 0,
            finished: false,
        })
    }

    pub fn finish(self) -> Result<()> {
        if self.has_next_column() {
            return Err(ErrorCode::BadArguments(format!(
                "FuseLowLevelDataReader read {} of {} logical columns",
                self.next_field,
                self.block.schema.num_fields()
            )));
        }
        Ok(())
    }
}

/// Page-driven reader for one physical Parquet leaf.
///
/// The decoder pulls fixed-size byte windows only when it needs more bytes for the
/// next page. It never reads another data page after producing a non-empty batch.
struct FuseLowLevelLeafReader {
    column_id: ColumnId,
    reader: Box<dyn ArrayReader>,
    expected_rows: usize,
    rows_decoded: usize,
    finished: bool,
}

impl FuseLowLevelLeafReader {
    fn create(
        context: &FuseLowLevelBlockReader,
        column_id: ColumnId,
        leaf_index: usize,
        range: Range<u64>,
        num_values: u64,
    ) -> Result<Self> {
        let row_group = ParquetLeafRowGroupAdapter::try_create(
            context, column_id, leaf_index, range, num_values,
        )?;
        let mask =
            ProjectionMask::leaves(context.parquet_schema.as_ref(), std::iter::once(leaf_index));
        let levels = parquet_to_arrow_field_levels(
            context.parquet_schema.as_ref(),
            mask,
            Some(context.arrow_schema.fields()),
        )?;
        let metrics = ArrowReaderMetrics::disabled();
        let reader = ArrayReaderBuilder::new(&row_group, &metrics)
            .with_parquet_metadata(context.parquet_metadata.as_ref())
            .build_array_reader_from_levels(&levels)?;

        Ok(Self {
            column_id,
            reader,
            expected_rows: context.row_count,
            rows_decoded: 0,
            finished: false,
        })
    }

    pub fn read(&mut self) -> Result<Option<ArrayRef>> {
        if self.finished {
            return Ok(None);
        }

        let rows = self.reader.read_page_records()?;
        let array = self.reader.consume_batch()?;
        let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "physical leaf {} did not produce a struct array",
                self.column_id
            )));
        };
        if struct_array.num_columns() != 1 || struct_array.len() != rows {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "physical leaf {} produced {} columns and {} rows, expected one column and {rows} rows",
                self.column_id,
                struct_array.num_columns(),
                struct_array.len()
            )));
        }

        if rows == 0 {
            if self.rows_decoded != self.expected_rows {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "physical leaf {} decoded {} rows, expected {}",
                    self.column_id, self.rows_decoded, self.expected_rows
                )));
            }
            self.finished = true;
            return Ok(None);
        }

        self.rows_decoded += rows;
        if self.rows_decoded > self.expected_rows {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "physical leaf {} decoded {} rows, expected {}",
                self.column_id, self.rows_decoded, self.expected_rows
            )));
        }
        Ok(Some(struct_array.column(0).clone()))
    }

    fn finish(mut self) -> Result<()> {
        if !self.finished {
            if self.read()?.is_some() {
                return Err(ErrorCode::BadArguments(format!(
                    "physical leaf {} was not fully consumed",
                    self.column_id
                )));
            }
        }
        Ok(())
    }
}

struct PhysicalColumnReader {
    arrow_field: Arc<Field>,
    leaves: Vec<FuseLowLevelLeafReader>,
    current: Vec<Option<ArrayRef>>,
}

impl PhysicalColumnReader {
    fn read(
        &mut self,
        field: &TableField,
        rows_returned: usize,
        expected_rows: usize,
    ) -> Result<Column> {
        for (leaf, current) in self.leaves.iter_mut().zip(&mut self.current) {
            if current.is_none() {
                *current = leaf.read()?;
            }
        }
        let mut rows = usize::MAX;
        for current in &self.current {
            let Some(array) = current else {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "column {} reached EOF after {} of {} rows",
                    field.name(),
                    rows_returned,
                    expected_rows
                )));
            };
            rows = rows.min(array.len());
        }
        debug_assert_ne!(rows, usize::MAX);
        debug_assert!(rows > 0);

        let mut branches = Vec::with_capacity(self.current.len());
        for current in &mut self.current {
            let array = current.take().expect("filled physical leaf");
            if array.len() == rows {
                branches.push(array);
            } else {
                branches.push(array.slice(0, rows));
                *current = Some(array.slice(rows, array.len() - rows));
            }
        }

        let array = merge_projected_arrays(self.arrow_field.as_ref(), &branches)?;
        let data_type = DataType::from(field.data_type());
        Column::from_arrow_rs(array, &data_type)
    }

    fn ensure_eof(&mut self, field: &TableField) -> Result<()> {
        for (leaf, current) in self.leaves.iter_mut().zip(&mut self.current) {
            if current.is_some() || leaf.read()?.is_some() {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "column {} contains rows after the declared row count",
                    field.name()
                )));
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        for leaf in self.leaves {
            leaf.finish()?;
        }
        Ok(())
    }
}

/// Stream metadata reader, optionally overlaying a persisted physical column.
struct StreamColumnReader {
    column_id: ColumnId,
    source_location: String,
    table_version: u64,
    physical: Option<PhysicalColumnReader>,
}

impl StreamColumnReader {
    fn read(
        &mut self,
        field: &TableField,
        rows_returned: usize,
        expected_rows: usize,
    ) -> Result<Column> {
        let persisted = match &mut self.physical {
            Some(physical) => Some(physical.read(field, rows_returned, expected_rows)?),
            None => None,
        };
        let rows = persisted
            .as_ref()
            .map_or(expected_rows - rows_returned, Column::len);
        materialize_stream_field(
            self.column_id,
            persisted,
            &self.source_location,
            rows_returned..rows_returned + rows,
            self.table_version,
        )
    }

    fn ensure_eof(&mut self, field: &TableField) -> Result<()> {
        match &mut self.physical {
            Some(physical) => physical.ensure_eof(field),
            None => Ok(()),
        }
    }

    fn finish(self) -> Result<()> {
        match self.physical {
            Some(physical) => physical.finish(),
            None => Ok(()),
        }
    }
}

enum FuseLowLevelColumnSource {
    Physical(PhysicalColumnReader),
    Default { scalar: Scalar, data_type: DataType },
    Stream(StreamColumnReader),
}

impl FuseLowLevelColumnSource {
    fn read(
        &mut self,
        field: &TableField,
        rows_returned: usize,
        expected_rows: usize,
    ) -> Result<Column> {
        match self {
            Self::Physical(reader) => reader.read(field, rows_returned, expected_rows),
            Self::Default { scalar, data_type } => Ok(ColumnBuilder::repeat(
                &scalar.as_ref(),
                expected_rows - rows_returned,
                data_type,
            )
            .build()),
            Self::Stream(reader) => reader.read(field, rows_returned, expected_rows),
        }
    }

    fn ensure_eof(&mut self, field: &TableField) -> Result<()> {
        match self {
            Self::Physical(reader) => reader.ensure_eof(field),
            Self::Default { .. } => Ok(()),
            Self::Stream(reader) => reader.ensure_eof(field),
        }
    }

    fn finish(self) -> Result<()> {
        match self {
            Self::Physical(reader) => reader.finish(),
            Self::Default { .. } => Ok(()),
            Self::Stream(reader) => reader.finish(),
        }
    }
}

/// Active streaming reader for one logical table column.
pub struct FuseLowLevelColumnReader {
    data: FuseLowLevelDataReader,
    field: TableField,
    source: FuseLowLevelColumnSource,
    expected_rows: usize,
    rows_returned: usize,
    finished: bool,
}

impl FuseLowLevelColumnReader {
    pub fn field(&self) -> &TableField {
        &self.field
    }

    /// Return the next page-driven physical batch or the synthesized default batch.
    pub fn read(&mut self) -> Result<Option<Column>> {
        if self.finished {
            return Ok(None);
        }
        if self.rows_returned == self.expected_rows {
            self.source.ensure_eof(&self.field)?;
            self.finished = true;
            return Ok(None);
        }

        let column = self
            .source
            .read(&self.field, self.rows_returned, self.expected_rows)?;

        self.rows_returned += column.len();
        if self.rows_returned > self.expected_rows {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "column {} returned {} rows, expected {}",
                self.field.name(),
                self.rows_returned,
                self.expected_rows
            )));
        }
        Ok(Some(column))
    }

    pub fn finish(mut self) -> Result<FuseLowLevelDataReader> {
        if !self.finished && self.read()?.is_some() {
            return Err(ErrorCode::BadArguments(format!(
                "column {} was not fully consumed",
                self.field.name()
            )));
        }
        self.source.finish()?;
        Ok(self.data)
    }
}

/// Fixed-size batching over one low-level logical-column stream.
pub struct FuseLowLevelColumnBatchReader {
    reader: FuseLowLevelColumnReader,
    buffered: Option<Column>,
    data_type: DataType,
    position: usize,
    expected_rows: usize,
}

impl FuseLowLevelColumnBatchReader {
    fn new(reader: FuseLowLevelColumnReader) -> Self {
        let data_type = DataType::from(reader.field.data_type());
        let expected_rows = reader.expected_rows;
        Self {
            reader,
            buffered: None,
            data_type,
            position: 0,
            expected_rows,
        }
    }

    pub fn read_rows(&mut self, rows: usize) -> Result<Column> {
        let Some(end) = self.position.checked_add(rows) else {
            return Err(ErrorCode::Internal(
                "low-level column batch position overflowed usize",
            ));
        };
        if end > self.expected_rows {
            return Err(ErrorCode::BadArguments(format!(
                "column {} requested rows {}..{}, expected at most {}",
                self.reader.field.name(),
                self.position,
                end,
                self.expected_rows
            )));
        }
        if rows == 0 {
            return Ok(ColumnBuilder::with_capacity(&self.data_type, 0).build());
        }

        let mut remaining = rows;
        let mut parts = Vec::new();
        while remaining > 0 {
            let column = match self.buffered.take() {
                Some(column) => column,
                None => {
                    let Some(column) = self.reader.read()? else {
                        return Err(ErrorCode::ParquetFileInvalid(format!(
                            "column {} reached EOF while assembling a batch",
                            self.reader.field.name()
                        )));
                    };
                    column
                }
            };
            let take = remaining.min(column.len());
            parts.push(column.slice(0..take));
            if take < column.len() {
                self.buffered = Some(column.slice(take..column.len()));
            }
            remaining -= take;
        }
        self.position = end;
        if parts.len() == 1 {
            Ok(parts.pop().expect("one logical-column batch"))
        } else {
            Column::concat_columns(parts.into_iter())
        }
    }

    pub fn finish(self) -> Result<()> {
        if self.position != self.expected_rows {
            return Err(ErrorCode::BadArguments(format!(
                "column {} returned {} of {} rows",
                self.reader.field.name(),
                self.position,
                self.expected_rows
            )));
        }
        if self.buffered.is_some() {
            return Err(ErrorCode::Internal(format!(
                "column {} retained rows after its declared row count",
                self.reader.field.name()
            )));
        }
        self.reader.finish()?.finish()
    }
}

/// Reads configured cluster-key dependencies and evaluates key expressions by row batch.
pub struct FuseLowLevelClusterKeyReader {
    block: FuseLowLevelBlockReader,
    exprs: Vec<Expr<usize>>,
    fields: BTreeSet<usize>,
    func_ctx: FunctionContext,
    key_readers: Vec<(usize, FuseLowLevelColumnBatchReader)>,
    payload_readers: HashMap<usize, FuseLowLevelColumnBatchReader>,
}

impl FuseLowLevelClusterKeyReader {
    pub fn read_rows(&mut self, rows: usize) -> Result<(Vec<Column>, HashMap<usize, Column>)> {
        let mut source_columns = HashMap::with_capacity(self.key_readers.len());
        for (field_index, reader) in &mut self.key_readers {
            source_columns.insert(*field_index, reader.read_rows(rows)?);
        }
        let keys = evaluate_cluster_keys(&source_columns, &self.exprs, &self.func_ctx, rows)?;

        Ok((keys, source_columns))
    }

    pub fn read_column_rows(&mut self, field_index: usize, rows: usize) -> Result<Column> {
        if self.fields.contains(&field_index) {
            return Err(ErrorCode::BadArguments(format!(
                "cluster-key field {field_index} must be reused from the key batch"
            )));
        }
        if !self.payload_readers.contains_key(&field_index) {
            let reader = self.block.create_column_batch_reader(field_index)?;
            self.payload_readers.insert(field_index, reader);
        }
        self.payload_readers
            .get_mut(&field_index)
            .expect("payload reader inserted")
            .read_rows(rows)
    }

    pub fn finish(self) -> Result<()> {
        for (_, reader) in self.key_readers {
            reader.finish()?;
        }
        for (_, reader) in self.payload_readers {
            reader.finish()?;
        }
        Ok(())
    }
}

fn evaluate_cluster_keys(
    source_columns: &HashMap<usize, Column>,
    exprs: &[Expr<usize>],
    func_ctx: &FunctionContext,
    rows: usize,
) -> Result<Vec<Column>> {
    let mut fields = source_columns.keys().copied().collect::<Vec<_>>();
    fields.sort_unstable();

    let mut positions = HashMap::with_capacity(fields.len());
    let mut columns = Vec::with_capacity(fields.len());
    for (position, field) in fields.into_iter().enumerate() {
        positions.insert(field, position);
        columns.push(source_columns[&field].clone());
    }

    let block = DataBlock::new_from_columns(columns);
    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let mut keys = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let projected = expr.project_column_ref(|field| match positions.get(field) {
            Some(position) => Ok(*position),
            None => Err(ErrorCode::Internal(format!(
                "cluster-key dependency field {field} is missing"
            ))),
        })?;
        let value = evaluator.run(&projected)?;
        keys.push(value.into_full_column(projected.data_type(), rows));
    }
    Ok(keys)
}

fn is_origin_column(column_id: ColumnId) -> bool {
    matches!(
        column_id,
        ORIGIN_VERSION_COLUMN_ID | ORIGIN_BLOCK_ID_COLUMN_ID | ORIGIN_BLOCK_ROW_NUM_COLUMN_ID
    )
}

fn materialize_stream_field(
    column_id: ColumnId,
    persisted: Option<Column>,
    source_location: &str,
    source_range: Range<usize>,
    table_version: u64,
) -> Result<Column> {
    let generated = match column_id {
        ORIGIN_VERSION_COLUMN_ID => Scalar::Number(NumberScalar::UInt64(table_version)),
        ORIGIN_BLOCK_ID_COLUMN_ID => Scalar::Decimal(DecimalScalar::Decimal128(
            block_id_from_location(source_location)?,
            DecimalSize::default_128(),
        )),
        ORIGIN_BLOCK_ROW_NUM_COLUMN_ID => Scalar::Number(NumberScalar::UInt64(0)),
        _ => {
            return Err(ErrorCode::Internal(format!(
                "column {column_id} is not a stream origin column"
            )));
        }
    };
    let data_type = match column_id {
        ORIGIN_VERSION_COLUMN_ID | ORIGIN_BLOCK_ROW_NUM_COLUMN_ID => {
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)))
        }
        ORIGIN_BLOCK_ID_COLUMN_ID => {
            DataType::Nullable(Box::new(DataType::Decimal(DecimalSize::default_128())))
        }
        _ => unreachable!(),
    };
    let rows = source_range.len();
    let mut builder = ColumnBuilder::with_capacity(&data_type, rows);
    for (local_row, source_row) in source_range.enumerate() {
        let persisted_value = if let Some(column) = &persisted {
            let value = unsafe { column.index_unchecked(local_row) };
            if matches!(value, ScalarRef::Null) {
                None
            } else {
                Some(value)
            }
        } else {
            None
        };
        if let Some(value) = persisted_value {
            builder.push(value);
        } else if column_id == ORIGIN_BLOCK_ROW_NUM_COLUMN_ID {
            builder.push(ScalarRef::Number(NumberScalar::UInt64(source_row as u64)));
        } else {
            builder.push(generated.as_ref());
        }
    }
    Ok(builder.build())
}

fn merge_projected_arrays(field: &Field, arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let Some(first) = arrays.first() else {
        return Err(ErrorCode::Internal("cannot merge an empty leaf projection"));
    };
    for array in arrays {
        if array.len() != first.len() {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "projected leaves of {} have inconsistent lengths",
                field.name()
            )));
        }
    }

    match field.data_type() {
        ArrowDataType::Struct(fields) => {
            let mut structs = Vec::with_capacity(arrays.len());
            let mut nulls = Vec::with_capacity(arrays.len());
            for array in arrays {
                let Some(value) = array.as_any().downcast_ref::<StructArray>() else {
                    return Err(ErrorCode::ParquetFileInvalid(format!(
                        "projected leaf of {} is not a struct",
                        field.name()
                    )));
                };
                nulls.push(value.nulls());
                structs.push(value);
            }
            ensure_matching_nulls(field.name(), &nulls)?;

            let mut children = Vec::with_capacity(fields.len());
            for child in fields {
                let mut projected = Vec::with_capacity(structs.len());
                for array in &structs {
                    if let Some(column) = array.column_by_name(child.name()) {
                        projected.push(column.clone());
                    }
                }
                let merged = merge_projected_arrays(child.as_ref(), &projected)?;
                children.push(merged);
            }

            let array =
                StructArray::try_new(fields.clone(), children, structs[0].nulls().cloned())?;
            Ok(Arc::new(array))
        }
        ArrowDataType::List(child) => {
            let mut offsets = Vec::with_capacity(arrays.len());
            let mut nulls = Vec::with_capacity(arrays.len());
            let mut values = Vec::with_capacity(arrays.len());
            for array in arrays {
                let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
                    return Err(ErrorCode::ParquetFileInvalid(format!(
                        "projected leaf of {} is not a list",
                        field.name()
                    )));
                };
                let (visible_offsets, visible_values) =
                    visible_repeated_values(field.name(), list.offsets(), list.values())?;
                offsets.push(visible_offsets);
                nulls.push(list.nulls());
                values.push(visible_values);
            }
            ensure_matching_offsets(field.name(), &offsets)?;
            ensure_matching_nulls(field.name(), &nulls)?;
            let values = merge_projected_arrays(child.as_ref(), &values)?;
            let array =
                ListArray::try_new(child.clone(), offsets[0].clone(), values, nulls[0].cloned())?;
            Ok(Arc::new(array))
        }
        ArrowDataType::LargeList(child) => {
            let mut offsets = Vec::with_capacity(arrays.len());
            let mut nulls = Vec::with_capacity(arrays.len());
            let mut values = Vec::with_capacity(arrays.len());
            for array in arrays {
                let Some(list) = array.as_any().downcast_ref::<LargeListArray>() else {
                    return Err(ErrorCode::ParquetFileInvalid(format!(
                        "projected leaf of {} is not a large list",
                        field.name()
                    )));
                };
                let (visible_offsets, visible_values) =
                    visible_repeated_values(field.name(), list.offsets(), list.values())?;
                offsets.push(visible_offsets);
                nulls.push(list.nulls());
                values.push(visible_values);
            }
            ensure_matching_offsets(field.name(), &offsets)?;
            ensure_matching_nulls(field.name(), &nulls)?;
            let values = merge_projected_arrays(child.as_ref(), &values)?;
            let array = LargeListArray::try_new(
                child.clone(),
                offsets[0].clone(),
                values,
                nulls[0].cloned(),
            )?;
            Ok(Arc::new(array))
        }
        ArrowDataType::FixedSizeList(child, size) => {
            let mut lists = Vec::with_capacity(arrays.len());
            let mut nulls = Vec::with_capacity(arrays.len());
            let mut values = Vec::with_capacity(arrays.len());
            for array in arrays {
                let Some(value) = array.as_any().downcast_ref::<FixedSizeListArray>() else {
                    return Err(ErrorCode::ParquetFileInvalid(format!(
                        "projected leaf of {} is not a fixed-size list",
                        field.name()
                    )));
                };
                nulls.push(value.nulls());
                values.push(value.values().clone());
                lists.push(value);
            }
            ensure_matching_nulls(field.name(), &nulls)?;
            let values = merge_projected_arrays(child.as_ref(), &values)?;
            let array = FixedSizeListArray::try_new(
                child.clone(),
                *size,
                values,
                lists[0].nulls().cloned(),
            )?;
            Ok(Arc::new(array))
        }
        ArrowDataType::Map(entries, ordered) => {
            let mut offsets = Vec::with_capacity(arrays.len());
            let mut nulls = Vec::with_capacity(arrays.len());
            let mut values = Vec::with_capacity(arrays.len());
            for array in arrays {
                let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
                    return Err(ErrorCode::ParquetFileInvalid(format!(
                        "projected leaf of map {} is not a list",
                        field.name()
                    )));
                };
                let (visible_offsets, visible_values) =
                    visible_repeated_values(field.name(), list.offsets(), list.values())?;
                offsets.push(visible_offsets);
                nulls.push(list.nulls());
                values.push(visible_values);
            }
            ensure_matching_offsets(field.name(), &offsets)?;
            ensure_matching_nulls(field.name(), &nulls)?;
            let entries_array = merge_projected_arrays(entries.as_ref(), &values)?;
            let Some(entries_array) = entries_array.as_any().downcast_ref::<StructArray>() else {
                return Err(ErrorCode::Internal("merged map entries are not a struct"));
            };
            let array = MapArray::try_new(
                entries.clone(),
                offsets[0].clone(),
                entries_array.clone(),
                nulls[0].cloned(),
                *ordered,
            )?;
            Ok(Arc::new(array))
        }
        _ => {
            if arrays.len() != 1 {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "scalar field {} was projected from {} leaves",
                    field.name(),
                    arrays.len()
                )));
            }
            Ok(first.clone())
        }
    }
}

fn visible_repeated_values<O: arrow_array::OffsetSizeTrait>(
    field: &str,
    offsets: &OffsetBuffer<O>,
    values: &ArrayRef,
) -> Result<(OffsetBuffer<O>, ArrayRef)> {
    let first = offsets[0].as_usize();
    let last = offsets[offsets.len() - 1].as_usize();
    if last < first || last > values.len() {
        return Err(ErrorCode::ParquetFileInvalid(format!(
            "projected leaf of {} has child span {}..{} outside {} values",
            field,
            first,
            last,
            values.len()
        )));
    }

    // Keep child data zero-copy while rebasing this repeated level to its visible span.
    // If the child is repeated too, merge_projected_arrays normalizes its span recursively.
    let normalized = OffsetBuffer::from_lengths(
        offsets
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).as_usize()),
    );
    Ok((normalized, values.slice(first, last - first)))
}

fn ensure_matching_offsets<O: arrow_array::OffsetSizeTrait>(
    field: &str,
    offsets: &[OffsetBuffer<O>],
) -> Result<()> {
    let Some(first) = offsets.first() else {
        return Err(ErrorCode::Internal(
            "cannot validate an empty offset projection",
        ));
    };
    for value in &offsets[1..] {
        if value != first {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "projected leaves of {field} have inconsistent offsets"
            )));
        }
    }
    Ok(())
}

fn ensure_matching_nulls(field: &str, nulls: &[Option<&arrow::buffer::NullBuffer>]) -> Result<()> {
    let Some(first) = nulls.first() else {
        return Err(ErrorCode::Internal(
            "cannot validate an empty null projection",
        ));
    };
    for value in &nulls[1..] {
        if value != first {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "projected leaves of {field} have inconsistent nullability"
            )));
        }
    }
    Ok(())
}

struct ForwardState {
    input: ChunkedRangeReader,
    position: u64,
    len: u64,
}

struct ForwardChunkReader {
    state: Arc<Mutex<ForwardState>>,
}

impl ForwardChunkReader {
    fn new(input: ChunkedRangeReader, len: u64) -> Self {
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

struct ParquetLeafRowGroupAdapter {
    num_rows: usize,
    leaf_index: usize,
    chunk: Arc<ForwardChunkReader>,
    chunk_metadata: ColumnChunkMetaData,
    metadata: Arc<ParquetMetaData>,
}

impl ParquetLeafRowGroupAdapter {
    fn try_create(
        reader: &FuseLowLevelBlockReader,
        column_id: ColumnId,
        leaf_index: usize,
        range: Range<u64>,
        num_values: u64,
    ) -> Result<Self> {
        let len = range.end - range.start;
        let chain = create_file_range_reader(
            reader.operator.clone(),
            reader.path.clone(),
            reader.block_meta.file_size,
            reader.max_prefetch,
            reader.window_size as u64,
            reader
                .window_size
                .saturating_mul(reader.max_prefetch.saturating_add(2)),
            reader.populate_cache,
        )?;
        let input = ChunkedRangeReader::with_range(
            chain,
            range,
            reader.window_size as u64,
            reader.max_prefetch,
        )?;
        let Ok(num_values) = i64::try_from(num_values) else {
            return Err(ErrorCode::BadArguments(format!(
                "Parquet leaf {column_id} num_values does not fit i64"
            )));
        };
        let Ok(compressed_size) = i64::try_from(len) else {
            return Err(ErrorCode::BadArguments(format!(
                "Parquet leaf {column_id} length {len} does not fit i64"
            )));
        };
        let descriptor = reader.parquet_schema.column(leaf_index);
        let chunk_metadata = ColumnChunkMetaData::builder(descriptor)
            .set_compression(reader.compression)
            .set_num_values(num_values)
            .set_data_page_offset(0)
            .set_total_compressed_size(compressed_size)
            .build()?;

        Ok(Self {
            num_rows: reader.row_count,
            leaf_index,
            chunk: Arc::new(ForwardChunkReader::new(input, len)),
            chunk_metadata,
            metadata: reader.parquet_metadata.clone(),
        })
    }
}

impl RowGroups for ParquetLeafRowGroupAdapter {
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn column_chunks(&self, index: usize) -> ParquetResult<Box<dyn PageIterator>> {
        if index != self.leaf_index {
            return Err(ParquetError::General(format!(
                "projected Parquet leaf {index} has no range reader"
            )));
        }
        let pages = SerializedPageReader::new(
            self.chunk.clone(),
            &self.chunk_metadata,
            self.num_rows,
            None,
        )?;
        Ok(Box::new(PageReaderOnce {
            reader: Some(Ok(Box::new(pages))),
        }))
    }

    fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
        Box::new(self.metadata.row_groups().iter())
    }

    fn metadata(&self) -> &ParquetMetaData {
        self.metadata.as_ref()
    }
}

struct PageReaderOnce {
    reader: Option<ParquetResult<Box<dyn PageReader>>>,
}

impl Iterator for PageReaderOnce {
    type Item = ParquetResult<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for PageReaderOnce {}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use std::collections::HashMap;

    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::AnyType;
    use databend_common_expression::types::ArrayColumn;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::number::UInt64Type;
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
        let mut options = FuseLowLevelBlockWriteOptions::new(
            FunctionContext::default(),
            operator,
            schema.clone(),
            WriteSettings {
                table_compression: compression,
                index_granularity: None,
                ..Default::default()
            },
            properties,
            (path.to_string(), 0),
        );
        options.set_statistics(
            schema
                .leaf_fields()
                .iter()
                .map(|field| (field.column_id(), DataType::from(field.data_type())))
                .collect(),
            Vec::new(),
            false,
        );
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
            .with_window_size(1)
            .with_max_prefetch(2)
    }

    fn read_all_column(reader: &mut FuseLowLevelColumnReader) -> Column {
        let mut parts = Vec::new();
        while let Some(column) = reader.read().unwrap() {
            parts.push(column);
        }
        Column::concat_columns(parts.into_iter()).unwrap()
    }

    fn read_columns(
        operator: Operator,
        schema: TableSchemaRef,
        meta: BlockMeta,
    ) -> (Vec<Column>, Vec<Vec<usize>>) {
        let mut data = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data();
        let mut columns = Vec::new();
        let mut boundaries = Vec::new();
        while data.has_next_column() {
            let mut reader = data.next_column().unwrap();
            let mut parts = Vec::new();
            let mut lengths = Vec::new();
            while let Some(column) = reader.read().unwrap() {
                lengths.push(column.len());
                parts.push(column);
            }
            columns.push(Column::concat_columns(parts.into_iter()).unwrap());
            boundaries.push(lengths);
            data = reader.finish().unwrap();
        }
        data.finish().unwrap();
        (columns, boundaries)
    }

    #[test]
    fn test_data_reader_returns_page_driven_column_batches() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "id",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let values = Int32Type::from_data(vec![10, 20, 30, 40, 50]);
        let meta = write_columns(
            operator.clone(),
            schema.clone(),
            "page-driven-read.parquet",
            std::slice::from_ref(&values),
        );
        let mut column = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data()
            .next_column()
            .unwrap();
        let actual = read_all_column(&mut column);
        assert_eq!(actual, values);
        assert!(column.read().unwrap().is_none());
        assert!(column.read().unwrap().is_none());
        column.finish().unwrap().finish().unwrap();
    }

    #[test]
    fn test_cluster_key_reader_batches_keys_and_payload_independently() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let (schema, columns) = test_data();
        let meta = write_columns(
            operator.clone(),
            schema.clone(),
            "cluster-key-reader.parquet",
            &columns,
        );
        let key_expr = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: "id".to_string(),
        });
        let options = read_options(operator, schema, meta)
            .with_cluster_keys(vec![key_expr], FunctionContext::default());
        let mut reader = FuseLowLevelBlockReader::create(options)
            .unwrap()
            .read_cluster_keys()
            .unwrap();

        let (first_keys, first_source_columns) = reader.read_rows(2).unwrap();
        assert_eq!(first_keys, vec![columns[0].slice(0..2)]);
        assert_eq!(first_source_columns[&0], columns[0].slice(0..2));
        assert_eq!(
            reader.read_column_rows(1, 2).unwrap(),
            columns[1].slice(0..2)
        );

        let (second_keys, second_source_columns) = reader.read_rows(3).unwrap();
        assert_eq!(second_keys, vec![columns[0].slice(2..5)]);
        assert_eq!(second_source_columns[&0], columns[0].slice(2..5));
        assert_eq!(
            reader.read_column_rows(1, 3).unwrap(),
            columns[1].slice(2..5)
        );
        assert!(reader.read_column_rows(0, 0).is_err());
        reader.finish().unwrap();
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
        assert_eq!(boundaries, vec![vec![1, 1]]);
    }

    #[test]
    fn test_low_level_reader_aligns_independent_nested_leaf_pages() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "pair",
            TableDataType::Tuple {
                fields_name: vec!["left".to_string(), "right".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::Number(NumberDataType::Int32),
                ],
            },
        )]));
        let expected = Column::Tuple(vec![
            Int32Type::from_data(vec![0, 1, 2, 3, 4]),
            Int32Type::from_data(vec![10, 11, 12, 13, 14]),
        ]);

        fn required_int32_page(values: &[i32]) -> Vec<u8> {
            let body = values
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect::<Vec<_>>();
            let header = PageHeader {
                type_: PageType::DATA_PAGE,
                uncompressed_page_size: body.len() as i32,
                compressed_page_size: body.len() as i32,
                crc: None,
                data_page_header: Some(DataPageHeader {
                    num_values: values.len() as i32,
                    encoding: parquet::format::Encoding::PLAIN,
                    definition_level_encoding: parquet::format::Encoding::RLE,
                    repetition_level_encoding: parquet::format::Encoding::RLE,
                    statistics: None,
                }),
                index_page_header: None,
                dictionary_page_header: None,
                data_page_header_v2: None,
            };
            let mut page = Vec::new();
            header
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut page))
                .unwrap();
            page.extend(body);
            page
        }

        let mut left = required_int32_page(&[0, 1]);
        left.extend(required_int32_page(&[2, 3]));
        left.extend(required_int32_page(&[4]));
        let mut right = required_int32_page(&[10, 11, 12]);
        right.extend(required_int32_page(&[13, 14]));
        let left_len = left.len() as u64;
        let right_len = right.len() as u64;
        left.extend(right);
        GlobalIORuntime::instance()
            .block_on(async {
                operator
                    .write("independent-pages.parquet", left)
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();

        let mut meta = write_columns(
            operator.clone(),
            schema.clone(),
            "independent-pages-baseline.parquet",
            std::slice::from_ref(&expected),
        );
        meta.location.0 = "independent-pages.parquet".to_string();
        meta.compression = databend_storages_common_table_meta::meta::Compression::None;
        for (index, column_id) in schema.to_leaf_column_ids().into_iter().enumerate() {
            let ColumnMeta::Parquet(mut column_meta) =
                meta.col_metas.get(&column_id).unwrap().clone();
            column_meta.offset = if index == 0 { 0 } else { left_len };
            column_meta.len = if index == 0 { left_len } else { right_len };
            column_meta.num_values = 5;
            meta.col_metas
                .insert(column_id, ColumnMeta::Parquet(column_meta));
        }

        let mut reader = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data()
            .next_column()
            .unwrap();
        let mut batches = Vec::new();
        let mut boundaries = Vec::new();
        while let Some(column) = reader.read().unwrap() {
            boundaries.push(column.len());
            batches.push(column);
        }
        reader.finish().unwrap().finish().unwrap();

        assert_eq!(boundaries, vec![2, 1, 1, 1]);
        assert_eq!(
            Column::concat_columns(batches.into_iter()).unwrap(),
            expected
        );
    }

    #[test]
    fn test_low_level_reader_aligns_independent_repeated_nested_leaf_pages() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "records",
            TableDataType::Array(Box::new(TableDataType::Tuple {
                fields_name: vec!["left".to_string(), "right".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::Number(NumberDataType::Int32),
                ],
            })),
        )]));
        let offsets: databend_common_column::buffer::Buffer<u64> =
            vec![0_u64, 2, 4, 6, 8, 10].into();
        let expected = Column::Array(Box::new(ArrayColumn::<AnyType>::new(
            Column::Tuple(vec![
                Int32Type::from_data((0..10).collect::<Vec<_>>()),
                Int32Type::from_data((10..20).collect::<Vec<_>>()),
            ]),
            offsets,
        )));
        let arrow_schema = Schema::from(schema.as_ref());
        let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema).unwrap();
        let left_descriptor = parquet_schema.column(0);
        let right_descriptor = parquet_schema.column(1);
        assert_eq!(
            left_descriptor.max_rep_level(),
            right_descriptor.max_rep_level()
        );
        assert_eq!(
            left_descriptor.max_def_level(),
            right_descriptor.max_def_level()
        );
        let max_rep = left_descriptor.max_rep_level();
        let max_def = left_descriptor.max_def_level();

        fn encode_levels(levels: &[i16], max_level: i16) -> Vec<u8> {
            let bit_width = (16 - (max_level as u16).leading_zeros()) as usize;
            let mut packed = vec![0_u8; bit_width];
            for (index, level) in levels.iter().enumerate() {
                let bit_offset = index * bit_width;
                let value = *level as u16;
                for bit in 0..bit_width {
                    if value & (1 << bit) != 0 {
                        let position = bit_offset + bit;
                        packed[position / 8] |= 1 << (position % 8);
                    }
                }
            }
            let mut encoded = vec![3];
            encoded.extend(packed);
            let mut result = (encoded.len() as u32).to_le_bytes().to_vec();
            result.extend(encoded);
            result
        }

        fn repeated_int32_page(rows: usize, max_rep: i16, max_def: i16, values: &[i32]) -> Vec<u8> {
            assert_eq!(values.len(), rows * 2);
            let mut repetition = Vec::with_capacity(values.len());
            for _ in 0..rows {
                repetition.push(0);
                repetition.push(max_rep);
            }
            let definition = vec![max_def; values.len()];
            let mut body = encode_levels(&repetition, max_rep);
            body.extend(encode_levels(&definition, max_def));
            body.extend(values.iter().flat_map(|value| value.to_le_bytes()));
            let header = PageHeader {
                type_: PageType::DATA_PAGE,
                uncompressed_page_size: body.len() as i32,
                compressed_page_size: body.len() as i32,
                crc: None,
                data_page_header: Some(DataPageHeader {
                    num_values: values.len() as i32,
                    encoding: parquet::format::Encoding::PLAIN,
                    definition_level_encoding: parquet::format::Encoding::RLE,
                    repetition_level_encoding: parquet::format::Encoding::RLE,
                    statistics: None,
                }),
                index_page_header: None,
                dictionary_page_header: None,
                data_page_header_v2: None,
            };
            let mut page = Vec::new();
            header
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut page))
                .unwrap();
            page.extend(body);
            page
        }

        let mut left = repeated_int32_page(3, max_rep, max_def, &(0..6).collect::<Vec<_>>());
        left.extend(repeated_int32_page(
            2,
            max_rep,
            max_def,
            &(6..10).collect::<Vec<_>>(),
        ));
        let mut right = repeated_int32_page(2, max_rep, max_def, &(10..14).collect::<Vec<_>>());
        right.extend(repeated_int32_page(
            3,
            max_rep,
            max_def,
            &(14..20).collect::<Vec<_>>(),
        ));
        let left_len = left.len() as u64;
        let right_len = right.len() as u64;
        left.extend(right);
        GlobalIORuntime::instance()
            .block_on(async {
                operator
                    .write("independent-repeated-pages.parquet", left)
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();

        let mut meta = write_columns(
            operator.clone(),
            schema.clone(),
            "independent-repeated-pages-baseline.parquet",
            std::slice::from_ref(&expected),
        );
        meta.location.0 = "independent-repeated-pages.parquet".to_string();
        meta.compression = databend_storages_common_table_meta::meta::Compression::None;
        for (index, column_id) in schema.to_leaf_column_ids().into_iter().enumerate() {
            let ColumnMeta::Parquet(mut column_meta) =
                meta.col_metas.get(&column_id).unwrap().clone();
            column_meta.offset = if index == 0 { 0 } else { left_len };
            column_meta.len = if index == 0 { left_len } else { right_len };
            column_meta.num_values = 10;
            meta.col_metas
                .insert(column_id, ColumnMeta::Parquet(column_meta));
        }

        let mut reader = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data()
            .next_column()
            .unwrap();
        let mut batches = Vec::new();
        let mut boundaries = Vec::new();
        while let Some(column) = reader.read().unwrap() {
            boundaries.push(column.len());
            batches.push(column);
        }
        reader.finish().unwrap().finish().unwrap();

        assert_eq!(boundaries, vec![2, 1, 2]);
        assert_eq!(
            Column::concat_columns(batches.into_iter()).unwrap(),
            expected
        );
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
        assert!(boundaries.iter().flatten().all(|rows| *rows > 0));
        assert!(
            boundaries
                .iter()
                .all(|lengths| lengths.iter().sum::<usize>() == 5)
        );
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
        let (columns, _) = read_columns(operator.clone(), schema.clone(), source_meta);

        let writer = FuseLowLevelBlockWriter::create(write_options(
            operator.clone(),
            schema.clone(),
            "copy.parquet",
        ))
        .unwrap();
        let mut data = writer.write_data().unwrap();
        for source in columns {
            let mut output = data.next_column().unwrap();
            output.write(&source).unwrap();
            data = output.finish().unwrap();
        }
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
        let (source_columns, _) = read_columns(operator.clone(), schema.clone(), source_meta);
        let take_indices = [0_u32, 2, 4];
        let selected = DataBlock::new_from_columns(source_columns.clone())
            .take(take_indices.as_slice())
            .unwrap();

        let writer = FuseLowLevelBlockWriter::create(write_options(
            operator.clone(),
            schema.clone(),
            "selected.parquet",
        ))
        .unwrap();
        let mut data = writer.write_data().unwrap();
        for entry in selected.columns() {
            let mut output = data.next_column().unwrap();
            output.write(&entry.to_column()).unwrap();
            data = output.finish().unwrap();
        }
        let selected_meta = data.finish().unwrap().finish().unwrap().block_meta;
        assert_eq!(selected_meta.row_count, 3);

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

        let data = FuseLowLevelBlockReader::create(read_options(
            operator.clone(),
            schema.clone(),
            meta.clone(),
        ))
        .unwrap()
        .read_data();
        assert!(data.finish().is_err());

        let mut column = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data()
            .next_column()
            .unwrap();
        assert!(column.read().unwrap().is_some());
        assert!(column.finish().is_err());
    }

    #[test]
    fn test_data_reader_requires_default_for_missing_field() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let (schema, columns) = test_data();
        let mut meta = write_columns(operator.clone(), schema.clone(), "source.parquet", &columns);
        let leaf = schema.to_leaf_column_ids()[0];
        meta.col_metas.remove(&leaf);

        let error = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data()
            .next_column()
            .err()
            .unwrap();
        assert!(error.message().contains("has no default value"));
    }

    #[test]
    fn test_data_reader_uses_schema_order_for_physical_default_and_stream_columns() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let source_schema = Arc::new(TableSchema::new(vec![TableField::new_from_column_id(
            "id",
            TableDataType::Number(NumberDataType::Int32),
            0,
        )]));
        let path = "root/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet";
        let values = Int32Type::from_data(vec![10, 20, 30]);
        let meta = write_columns(
            operator.clone(),
            source_schema,
            path,
            std::slice::from_ref(&values),
        );
        let schema = Arc::new(TableSchema::new_from_column_ids(
            vec![
                TableField::new_from_column_id(
                    "id",
                    TableDataType::Number(NumberDataType::Int32),
                    0,
                ),
                TableField::new_from_column_id(
                    "added",
                    TableDataType::Number(NumberDataType::Int32),
                    1,
                ),
                TableField::new_from_column_id(
                    "_origin_version",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                    ORIGIN_VERSION_COLUMN_ID,
                ),
                TableField::new_from_column_id(
                    "_origin_block_id",
                    TableDataType::Nullable(Box::new(TableDataType::Decimal(
                        DecimalDataType::Decimal128(DecimalSize::default_128()),
                    ))),
                    ORIGIN_BLOCK_ID_COLUMN_ID,
                ),
                TableField::new_from_column_id(
                    "_origin_block_row_num",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                    ORIGIN_BLOCK_ROW_NUM_COLUMN_ID,
                ),
            ],
            Default::default(),
            ORIGIN_BLOCK_ROW_NUM_COLUMN_ID + 1,
        ));
        let options = FuseLowLevelBlockReadOptions::new(operator, schema, Arc::new(meta))
            .with_default_values(vec![
                Scalar::Null,
                Scalar::Number(NumberScalar::Int32(99)),
                Scalar::Null,
                Scalar::Null,
                Scalar::Null,
            ])
            .with_stream_table_version(42);
        let mut data = FuseLowLevelBlockReader::create(options)
            .unwrap()
            .read_data();

        let expected = vec![
            ("id", vec![
                Scalar::Number(NumberScalar::Int32(10)),
                Scalar::Number(NumberScalar::Int32(20)),
                Scalar::Number(NumberScalar::Int32(30)),
            ]),
            ("added", vec![Scalar::Number(NumberScalar::Int32(99)); 3]),
            ("_origin_version", vec![
                Scalar::Number(NumberScalar::UInt64(
                    42
                ));
                3
            ]),
            ("_origin_block_id", vec![
                Scalar::Decimal(
                    DecimalScalar::Decimal128(
                        block_id_from_location(path).unwrap(),
                        DecimalSize::default_128(),
                    )
                );
                3
            ]),
            ("_origin_block_row_num", vec![
                Scalar::Number(NumberScalar::UInt64(0)),
                Scalar::Number(NumberScalar::UInt64(1)),
                Scalar::Number(NumberScalar::UInt64(2)),
            ]),
        ];

        for (name, expected_values) in expected {
            let mut column = data.next_column().unwrap();
            assert_eq!(column.field().name(), name);
            let actual = read_all_column(&mut column)
                .iter()
                .map(|value| value.to_owned())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected_values);
            data = column.finish().unwrap();
        }
        assert!(!data.has_next_column());
        data.finish().unwrap();
    }

    #[test]
    fn test_stream_reader_overlays_persisted_values_with_generated_metadata() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let field = TableField::new_from_column_id(
            "_origin_block_row_num",
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ORIGIN_BLOCK_ROW_NUM_COLUMN_ID,
        );
        let schema = Arc::new(TableSchema::new_from_column_ids(
            vec![field],
            Default::default(),
            ORIGIN_BLOCK_ROW_NUM_COLUMN_ID + 1,
        ));
        let path = "root/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet";
        let persisted = UInt64Type::from_opt_data(vec![Some(100), None, Some(300)]);
        let meta = write_columns(
            operator.clone(),
            schema.clone(),
            path,
            std::slice::from_ref(&persisted),
        );

        let mut options = FuseLowLevelBlockReadOptions::new(operator, schema, Arc::new(meta));
        options = options.with_default_values(vec![Scalar::Null]);
        options = options.with_stream_table_version(42);
        let block_reader = FuseLowLevelBlockReader::create(options).unwrap();
        let data_reader = block_reader.read_data();
        let mut column_reader = data_reader.next_column().unwrap();

        let mut batches = Vec::new();
        while let Some(batch) = column_reader.read().unwrap() {
            assert!(batch.len() > 0);
            batches.push(batch);
        }
        let column = Column::concat_columns(batches.into_iter()).unwrap();
        let actual = column
            .iter()
            .map(|value| value.to_owned())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![
            Scalar::Number(NumberScalar::UInt64(100)),
            Scalar::Number(NumberScalar::UInt64(1)),
            Scalar::Number(NumberScalar::UInt64(300)),
        ]);

        let data_reader = column_reader.finish().unwrap();
        data_reader.finish().unwrap();
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

        let mut column = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data()
            .next_column()
            .unwrap();
        let mut rows = 0;
        loop {
            match column.read() {
                Ok(Some(batch)) => rows += batch.len(),
                Ok(None) => panic!("row-count mismatch unexpectedly reached clean EOF"),
                Err(error) => {
                    assert_eq!(rows, 5);
                    assert!(error.message().contains("decoded 5 rows, expected 6"));
                    break;
                }
            }
        }
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

        let mut column = FuseLowLevelBlockReader::create(read_options(operator, schema, meta))
            .unwrap()
            .read_data()
            .next_column()
            .unwrap();
        assert!(column.read().unwrap().is_some());
        assert!(column.finish().is_err());
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
