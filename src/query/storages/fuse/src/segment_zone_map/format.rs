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

//! Parquet layout of the segment zone map file.
//!
//! Fields 0..3 describe the segment: `path`, `block_count`, and `fresh`
//! (whether the build that wrote the file added the row, or copied it from the
//! previous zone map). Each covered table column `c` then takes three adjacent
//! fields: `min_c` and `max_c` (nullable, of the column's non-null type) and
//! `null_count_c` (nullable UInt64). A NULL `null_count_c` means the segment has
//! no usable statistics for `c`; readers leave the column out of the statistics
//! map, which range pruning treats as an unknown domain. A non-NULL
//! `null_count_c` with NULL `min_c`/`max_c` means the column is entirely NULL
//! in that segment, as in the segment summary.
//!
//! Rows are grouped into row groups of at most [`ROW_GROUP_ROWS`] segments so
//! that neither the build nor a query has to hold the whole file in memory:
//! the writer streams finished row groups to storage, and readers decode one
//! row group at a time. Each column chunk records min/max statistics in the
//! footer, which lets a reader skip row groups a filter cannot match before
//! fetching them.
//!
//! The footer's key-value metadata records the covered columns with their
//! types and the admission ratio the build used. The snapshot only points at
//! the footer; readers fetch it first and then the column chunks they need.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::future::IntoFuture;
use std::ops::Range;
use std::sync::Arc;

use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentZoneMapMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::supported_stat_type;
use databend_storages_common_table_meta::table::TableCompression;
use futures::future::try_join_all;
use opendal::Operator;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::basic::Encoding;
use parquet::file::metadata::KeyValue;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics as ParquetStatistics;
use parquet::schema::types::SchemaDescriptor;
use serde::Deserialize;
use serde::Serialize;

use crate::io::read::DataItem;
use crate::io::read::column_chunks_to_record_batch;

/// Rows per row group: bounds the memory a build or a query holds for one file.
pub const ROW_GROUP_ROWS: usize = 16 * 1024;

/// Number of leading fields describing the segment itself: path, block_count, fresh.
const SEGMENT_FIELDS: usize = 3;
/// Fields per covered column: min, max, null_count.
const STAT_FIELDS: usize = 3;

const KV_COLUMNS: &str = "databend.segment_zone_map.columns";
const KV_RATIO: &str = "databend.segment_zone_map.ratio";

const WRITE_COMPRESSION: TableCompression = TableCompression::Zstd;
const READ_COMPRESSION: Compression = Compression::Zstd;
/// Parquet footer: 4-byte little-endian metadata length followed by `PAR1`.
const PARQUET_FOOTER_TAIL: usize = 8;

const U64: TableDataType = TableDataType::Number(NumberDataType::UInt64);

/// Type of the `null_count` fields: NULL marks unknown statistics.
fn null_count_type() -> TableDataType {
    U64.wrap_nullable()
}

/// A table leaf column stored in the segment zone map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoneMapColumn {
    pub column_id: ColumnId,
    pub data_type: TableDataType,
}

impl ZoneMapColumn {
    /// Type of the stored `min`/`max`: the column's non-null type, made nullable.
    fn value_type(&self) -> TableDataType {
        self.data_type.remove_nullable().wrap_nullable()
    }

    /// Whether the parquet statistics of this column's chunks bound its values:
    /// the chunk min/max of `min`/`max` then hold for every row of the row
    /// group. Floats are left out because parquet statistics skip NaN, and
    /// TimestampTz because it is stored packed in a decimal.
    fn chunk_stats_bound_values(&self) -> bool {
        match self.data_type.remove_nullable() {
            TableDataType::Number(number) => !number.is_float(),
            TableDataType::Decimal(_) => true,
            TableDataType::Date | TableDataType::Timestamp | TableDataType::String => true,
            _ => false,
        }
    }
}

/// Leaf columns of `schema` that carry min/max statistics.
pub fn zone_map_columns(schema: &TableSchema) -> Vec<ZoneMapColumn> {
    let mut columns = Vec::new();
    for field in schema.leaf_fields() {
        if supported_stat_type(&DataType::from(field.data_type())) {
            let data_type = field.data_type().clone();
            columns.push(ZoneMapColumn {
                column_id: field.column_id(),
                data_type,
            });
        }
    }
    columns
}

/// Field ids follow the field order, as all fields are primitive.
fn file_schema(columns: &[ZoneMapColumn]) -> TableSchema {
    let mut fields = vec![
        TableField::new("path", TableDataType::String),
        TableField::new("block_count", U64),
        TableField::new("fresh", TableDataType::Boolean),
    ];
    for column in columns {
        let (id, value_type) = (column.column_id, column.value_type());
        fields.push(TableField::new(&format!("min_{id}"), value_type.clone()));
        fields.push(TableField::new(&format!("max_{id}"), value_type));
        fields.push(TableField::new(
            &format!("null_count_{id}"),
            null_count_type(),
        ));
    }
    TableSchema::new(fields)
}

/// Field id of `min` of the covered column at `idx`; `max` and `null_count` follow.
fn stat_field_id(idx: usize) -> usize {
    SEGMENT_FIELDS + idx * STAT_FIELDS
}

fn u64_scalar(value: u64) -> ScalarRef<'static> {
    ScalarRef::Number(NumberScalar::UInt64(value))
}

fn column_builder(data_type: &TableDataType) -> ColumnBuilder {
    ColumnBuilder::with_capacity(&DataType::from(data_type), ROW_GROUP_ROWS)
}

struct StatColumnBuilder {
    /// Non-null type the stored scalars must have.
    value_type: DataType,
    min: ColumnBuilder,
    max: ColumnBuilder,
    null_count: ColumnBuilder,
}

impl StatColumnBuilder {
    fn new(column: &ZoneMapColumn) -> Self {
        let value_type = column.value_type();
        Self {
            value_type: DataType::from(&value_type).remove_nullable(),
            min: column_builder(&value_type),
            max: column_builder(&value_type),
            null_count: column_builder(&null_count_type()),
        }
    }

    fn accepts(&self, value: ScalarRef) -> bool {
        value == ScalarRef::Null || value.infer_data_type() == self.value_type
    }

    /// Statistics of another type than the column's are as unknown as missing ones.
    fn push(&mut self, stats: Option<&ColumnStatistics>) {
        match stats {
            Some(s) if self.accepts(s.min.as_ref()) && self.accepts(s.max.as_ref()) => {
                self.min.push(s.min.as_ref());
                self.max.push(s.max.as_ref());
                self.null_count.push(u64_scalar(s.null_count));
            }
            _ => self.push_unknown(),
        }
    }

    fn push_unknown(&mut self) {
        self.min.push(ScalarRef::Null);
        self.max.push(ScalarRef::Null);
        self.null_count.push(ScalarRef::Null);
    }

    fn push_from(&mut self, stats: &StatColumns, row: usize) {
        self.min.push(stats.min.index(row).unwrap());
        self.max.push(stats.max.index(row).unwrap());
        self.null_count.push(stats.null_count.index(row).unwrap());
    }

    /// Take the buffered rows as columns, leaving the builder empty.
    fn take(&mut self, out: &mut Vec<Column>) {
        for builder in [&mut self.min, &mut self.max, &mut self.null_count] {
            let data_type = builder.data_type();
            let rows = std::mem::replace(builder, ColumnBuilder::with_capacity(&data_type, 0));
            out.push(rows.build());
        }
    }
}

/// For each column of a writer, the position of the same column (same id and
/// type) in a previous zone map, if it was read. See [`SegmentZoneMapWriter::copy_sources`].
pub struct CopySources(Vec<Option<usize>>);

/// Writes zone map rows in row groups of [`ROW_GROUP_ROWS`], streaming each
/// finished row group to storage so a build holds one row group at a time.
pub struct SegmentZoneMapWriter {
    columns: Vec<ZoneMapColumn>,
    arrow_schema: Arc<Schema>,
    /// Encodes into an in-memory buffer that is drained after every row group.
    parquet: ArrowWriter<Vec<u8>>,
    output: opendal::Writer,
    location: Location,
    paths: ColumnBuilder,
    block_counts: ColumnBuilder,
    fresh: ColumnBuilder,
    stats: Vec<StatColumnBuilder>,
    row_group_rows: usize,
    buffered: usize,
    num_rows: usize,
    written_bytes: u64,
}

impl SegmentZoneMapWriter {
    /// `ratio` is the admission ratio of this build, recorded for the next one.
    pub async fn create(
        operator: &Operator,
        location: Location,
        columns: Vec<ZoneMapColumn>,
        ratio: f64,
    ) -> Result<Self> {
        let metadata = vec![
            KeyValue::new(KV_COLUMNS.to_string(), serde_json::to_string(&columns)?),
            KeyValue::new(KV_RATIO.to_string(), ratio.to_string()),
        ];
        // Chunk statistics let readers skip whole row groups; see
        // `SegmentZoneMapFile::row_group_stats`.
        let props = WriterProperties::builder()
            .set_compression(WRITE_COMPRESSION.into())
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .set_encoding(Encoding::PLAIN)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_bloom_filter_enabled(false)
            .set_key_value_metadata(Some(metadata))
            .build();
        let arrow_schema = Arc::new(Schema::from(&file_schema(&columns)));
        let parquet = ArrowWriter::try_new(Vec::new(), arrow_schema.clone(), Some(props))?;
        let output = operator.writer(&location.0).await?;
        let stats = columns.iter().map(StatColumnBuilder::new).collect();
        Ok(Self {
            columns,
            arrow_schema,
            parquet,
            output,
            location,
            paths: column_builder(&TableDataType::String),
            block_counts: column_builder(&U64),
            fresh: column_builder(&TableDataType::Boolean),
            stats,
            row_group_rows: ROW_GROUP_ROWS,
            buffered: 0,
            num_rows: 0,
            written_bytes: 0,
        })
    }

    /// Smaller row groups, so tests cover several of them cheaply.
    #[cfg(test)]
    pub fn with_row_group_rows(mut self, rows: usize) -> Self {
        self.row_group_rows = rows;
        self
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Append a fresh row from a segment summary.
    pub async fn push_summary(&mut self, path: &str, summary: &Statistics) -> Result<()> {
        self.paths.push(ScalarRef::String(path));
        self.block_counts.push(u64_scalar(summary.block_count));
        self.fresh.push(ScalarRef::Boolean(true));
        for (column, builder) in self.columns.iter().zip(&mut self.stats) {
            builder.push(summary.col_stats.get(&column.column_id));
        }
        self.row_done().await
    }

    /// Match this writer's columns against `previous`. A column is copied only
    /// when `previous` stores it with the same id and type and it was read; a
    /// column whose type changed, or that `previous` lacks, is written as unknown.
    pub fn copy_sources(&self, previous: &SegmentZoneMapData) -> CopySources {
        let mut sources = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let idx = previous.columns.iter().position(|c| c == column);
            sources.push(idx.filter(|idx| previous.stats[*idx].is_some()));
        }
        CopySources(sources)
    }

    /// Copy row `row` of `previous` as a non-fresh row.
    pub async fn push_copy(
        &mut self,
        previous: &SegmentZoneMapData,
        row: usize,
        sources: &CopySources,
    ) -> Result<()> {
        let (path, block_count) = (previous.path(row), previous.block_count(row));
        self.paths.push(ScalarRef::String(path));
        self.block_counts.push(u64_scalar(block_count));
        self.fresh.push(ScalarRef::Boolean(false));
        for (builder, source) in self.stats.iter_mut().zip(&sources.0) {
            match source.and_then(|idx| previous.stats[idx].as_ref()) {
                Some(stats) => builder.push_from(stats, row),
                None => builder.push_unknown(),
            }
        }
        self.row_done().await
    }

    async fn row_done(&mut self) -> Result<()> {
        self.buffered += 1;
        self.num_rows += 1;
        match self.buffered == self.row_group_rows {
            true => self.flush_row_group().await,
            false => Ok(()),
        }
    }

    /// Encode the buffered rows as one row group and send it to storage.
    async fn flush_row_group(&mut self) -> Result<()> {
        if self.buffered == 0 {
            return Ok(());
        }
        let mut columns = Vec::with_capacity(self.arrow_schema.fields().len());
        for builder in [&mut self.paths, &mut self.block_counts, &mut self.fresh] {
            let data_type = builder.data_type();
            let rows = std::mem::replace(builder, ColumnBuilder::with_capacity(&data_type, 0));
            columns.push(rows.build());
        }
        for stats in &mut self.stats {
            stats.take(&mut columns);
        }
        let block = DataBlock::new_from_columns(columns);
        let batch = block.to_record_batch_with_arrow_schema(&self.arrow_schema)?;
        self.parquet.write(&batch)?;
        self.parquet.flush()?;
        self.buffered = 0;
        self.send().await
    }

    /// Send what the parquet writer has encoded so far.
    async fn send(&mut self) -> Result<()> {
        let bytes = std::mem::take(self.parquet.inner_mut());
        self.written_bytes += bytes.len() as u64;
        Ok(self.output.write(bytes).await?)
    }

    /// Write the last row group and the footer, returning the pointer to the file.
    pub async fn finish(mut self) -> Result<SegmentZoneMapMeta> {
        match self.finish_inner().await {
            Ok(footer_offset) => Ok(SegmentZoneMapMeta {
                location: self.location,
                footer_offset,
            }),
            Err(e) => {
                self.abort().await;
                Err(e)
            }
        }
    }

    async fn finish_inner(&mut self) -> Result<u64> {
        if self.num_rows == 0 {
            return Err(ErrorCode::Internal("segment zone map has no rows"));
        }
        self.flush_row_group().await?;
        self.parquet.finish()?;
        // The buffer now holds the footer and its 8-byte tail.
        let tail = self.parquet.inner();
        let footer_len = tail.len().saturating_sub(PARQUET_FOOTER_TAIL);
        let Some(len) = parse_footer_tail(&tail[footer_len..]) else {
            return Err(ErrorCode::Internal(
                "serialized segment zone map is not parquet",
            ));
        };
        if len > footer_len {
            return Err(ErrorCode::Internal(
                "serialized segment zone map footer is longer than the file",
            ));
        }
        let footer_offset = self.written_bytes + (footer_len - len) as u64;
        self.send().await?;
        self.output.close().await?;
        Ok(footer_offset)
    }

    /// Discard the file; for a build that failed after writing rows.
    pub async fn abort(mut self) {
        if let Err(e) = self.output.abort().await {
            log::warn!("failed to abort segment zone map {}: {e}", self.location.0);
        }
    }
}

/// Footer metadata length from the 8 trailing bytes of a parquet file.
fn parse_footer_tail(tail: &[u8]) -> Option<usize> {
    if tail.len() != PARQUET_FOOTER_TAIL || &tail[4..] != b"PAR1" {
        return None;
    }
    Some(u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize)
}

fn kv_value<'a>(parquet: &'a ParquetMetaData, key: &str) -> Option<&'a str> {
    let kvs = parquet.file_metadata().key_value_metadata()?;
    kvs.iter().find(|kv| kv.key == key)?.value.as_deref()
}

struct StatColumns {
    min: Column,
    max: Column,
    null_count: Column,
}

/// An opened zone map file: its footer, parsed. Rows are read per row group.
pub struct SegmentZoneMapFile {
    operator: Operator,
    path: String,
    schema: TableSchema,
    parquet: ParquetMetaData,
    columns: Vec<ZoneMapColumn>,
    ratio: f64,
}

impl SegmentZoneMapFile {
    pub async fn open(operator: &Operator, meta: &SegmentZoneMapMeta) -> Result<Self> {
        let (path, version) = meta.location.clone();
        let invalid = |what: String| invalid(&path, what);
        // A file of another format version may lay its fields out differently.
        if version != SegmentZoneMapMeta::VERSION {
            return Err(invalid(format!("unsupported format version {version}")));
        }
        // One read to the end of the file: footer metadata, its length, magic.
        let tail = operator.read_with(&path).range(meta.footer_offset..);
        let tail = tail.await?.to_bytes();
        let footer_len = tail.len().saturating_sub(PARQUET_FOOTER_TAIL);
        if parse_footer_tail(&tail[footer_len..]) != Some(footer_len) {
            return Err(invalid("footer offset does not match the file".into()));
        }
        let parquet = ParquetMetaDataReader::decode_metadata(&tail[..footer_len])
            .map_err(|e| invalid(format!("footer: {e}")))?;

        let json = kv_value(&parquet, KV_COLUMNS).ok_or_else(|| invalid("no columns".into()))?;
        let columns: Vec<ZoneMapColumn> =
            serde_json::from_str(json).map_err(|e| invalid(format!("columns: {e}")))?;
        let ratio = kv_value(&parquet, KV_RATIO).ok_or_else(|| invalid("no ratio".into()))?;
        let ratio: f64 = ratio.parse().map_err(|e| invalid(format!("ratio: {e}")))?;

        let expected = SEGMENT_FIELDS + columns.len() * STAT_FIELDS;
        let fields = parquet.file_metadata().schema_descr().num_columns();
        let groups = parquet.num_row_groups();
        if groups == 0 || fields != expected {
            let found = format!("{groups} row groups of {fields} fields");
            return Err(invalid(format!("{found}, want 1 or more of {expected}")));
        }
        Ok(Self {
            operator: operator.clone(),
            path,
            schema: file_schema(&columns),
            parquet,
            columns,
            ratio,
        })
    }

    pub fn columns(&self) -> &[ZoneMapColumn] {
        &self.columns
    }

    /// Admission ratio the build that wrote this file used.
    pub fn ratio(&self) -> f64 {
        self.ratio
    }

    pub fn num_rows(&self) -> usize {
        self.parquet.file_metadata().num_rows() as usize
    }

    pub fn num_row_groups(&self) -> usize {
        self.parquet.num_row_groups()
    }

    /// Per row group, statistics of the given table columns that hold for every
    /// row of the group, from the chunk statistics in the footer: the least
    /// `min`, the greatest `max`, and a null count that is positive if any row
    /// has nulls. Columns are left out where a row of the group has unknown
    /// statistics, or where the chunk statistics do not bound the values; see
    /// [`ZoneMapColumn::chunk_stats_bound_values`].
    pub fn row_group_stats(&self, column_ids: &HashSet<ColumnId>) -> Vec<StatisticsOfColumns> {
        let groups = self.parquet.row_groups();
        let mut out = vec![StatisticsOfColumns::new(); groups.len()];
        let arrow_schema = Schema::from(&self.schema);
        let Ok(parquet_schema) = ArrowSchemaConverter::new().convert(&arrow_schema) else {
            return out;
        };
        let bounds =
            |field_id, max| self.chunk_bounds(field_id, &arrow_schema, &parquet_schema, max);
        for (idx, column) in self.columns.iter().enumerate() {
            if !column_ids.contains(&column.column_id) || !column.chunk_stats_bound_values() {
                continue;
            }
            let first = stat_field_id(idx);
            let (min, max, nulls) = (
                bounds(first, false),
                bounds(first + 1, true),
                bounds(first + 2, true),
            );
            let (Some(min), Some(max), Some(nulls)) = (min, max, nulls) else {
                continue;
            };
            for (g, (group, stats)) in groups.iter().zip(&mut out).enumerate() {
                let nulls_of = |field_id: usize| {
                    let chunk_stats = group.column(field_id).statistics();
                    chunk_stats.and_then(ParquetStatistics::null_count_opt)
                };
                // A NULL null_count is a row with unknown statistics.
                if nulls_of(first + 2) != Some(0) {
                    continue;
                }
                let (min, max) = (min.index(g).unwrap(), max.index(g).unwrap());
                let mut null_count = match nulls.index(g) {
                    Some(ScalarRef::Number(NumberScalar::UInt64(nulls))) => nulls,
                    _ => 1,
                };
                // Rows whose column is entirely NULL store NULL as min.
                null_count = null_count.max(u64::from(nulls_of(first) != Some(0)));
                let (min, max) = (min.to_owned(), max.to_owned());
                stats.insert(
                    column.column_id,
                    ColumnStatistics::new(min, max, null_count, 0, None),
                );
            }
        }
        out
    }

    /// The least (or, with `max`, the greatest) value of field `field_id` in
    /// each row group, from the chunk statistics; NULL where a group has none.
    fn chunk_bounds(
        &self,
        field_id: usize,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
        max: bool,
    ) -> Option<Column> {
        let field = &self.schema.fields()[field_id];
        let converter = StatisticsConverter::try_new(field.name(), arrow_schema, parquet_schema);
        let groups = self.parquet.row_groups();
        let array = match max {
            true => converter.ok()?.row_group_maxes(groups),
            false => converter.ok()?.row_group_mins(groups),
        };
        Column::from_arrow_rs(array.ok()?, &DataType::from(field.data_type())).ok()
    }

    /// Read the segment fields and the statistics of the given table columns
    /// of row group `group`.
    pub async fn read_row_group(
        &self,
        group: usize,
        column_ids: &HashSet<ColumnId>,
    ) -> Result<SegmentZoneMapData> {
        let mut selected = Vec::new();
        let mut field_ids: Vec<usize> = (0..SEGMENT_FIELDS).collect();
        for (idx, column) in self.columns.iter().enumerate() {
            if column_ids.contains(&column.column_id) {
                selected.push(idx);
                field_ids.extend(stat_field_id(idx)..stat_field_id(idx + 1));
            }
        }

        let row_group = self.parquet.row_group(group);
        let chunks = self.read_fields(row_group, &field_ids).await?;
        let (schema, rows, compression) = (&self.schema, row_group.num_rows(), &READ_COMPRESSION);
        let rows = usize::try_from(rows)?;
        let batch = column_chunks_to_record_batch(schema, rows, &chunks, compression, None, None)?;
        let column_of = |field_id: usize| -> Result<Column> {
            let field = &schema.fields()[field_id];
            let missing = || invalid(&self.path, format!("missing {}", field.name()));
            let array = batch.column_by_name(field.name()).ok_or_else(missing)?;
            Column::from_arrow_rs(array.clone(), &DataType::from(field.data_type()))
        };

        let mut stats = Vec::new();
        stats.resize_with(self.columns.len(), || None);
        for idx in selected {
            let first = stat_field_id(idx);
            stats[idx] = Some(StatColumns {
                min: column_of(first)?,
                max: column_of(first + 1)?,
                null_count: column_of(first + 2)?,
            });
        }
        Ok(SegmentZoneMapData {
            columns: self.columns.clone(),
            paths: column_of(0)?,
            block_counts: column_of(1)?,
            fresh: column_of(2)?,
            stats,
        })
    }

    /// Fetch the chunks of `field_ids` (ascending) of a row group. Adjacent
    /// fields are adjacent in the file: one request per run of them, all issued
    /// concurrently.
    async fn read_fields(
        &self,
        row_group: &RowGroupMetaData,
        field_ids: &[usize],
    ) -> Result<HashMap<ColumnId, DataItem<'_>>> {
        let bytes_of = |field_id: usize| -> Range<u64> {
            let (offset, len) = row_group.column(field_id).byte_range();
            offset..offset + len
        };
        // Runs of adjacent fields, each with the byte range covering them.
        let mut runs: Vec<(Range<usize>, Range<u64>)> = Vec::new();
        for &field_id in field_ids {
            let bytes = bytes_of(field_id);
            match runs.last_mut() {
                Some((fields, run)) if fields.end == field_id => {
                    fields.end += 1;
                    run.start = run.start.min(bytes.start);
                    run.end = run.end.max(bytes.end);
                }
                _ => runs.push((field_id..field_id + 1, bytes)),
            }
        }
        let read = |run: &Range<u64>| self.operator.read_with(&self.path).range(run.clone());
        let buffers = try_join_all(runs.iter().map(|(_, run)| read(run).into_future())).await?;

        let mut chunks = HashMap::with_capacity(field_ids.len());
        for ((fields, run), buffer) in runs.iter().zip(buffers) {
            for field_id in fields.clone() {
                let bytes = bytes_of(field_id);
                let start = (bytes.start - run.start) as usize;
                let end = (bytes.end - run.start) as usize;
                if end > buffer.len() {
                    return Err(invalid(&self.path, format!("truncated field {field_id}")));
                }
                chunks.insert(
                    field_id as ColumnId,
                    DataItem::RawData(buffer.slice(start..end)),
                );
            }
        }
        Ok(chunks)
    }
}

/// Rows of one row group: the segment fields plus the statistics that were read.
pub struct SegmentZoneMapData {
    columns: Vec<ZoneMapColumn>,
    paths: Column,
    block_counts: Column,
    fresh: Column,
    /// Aligned with `columns`; `None` for columns that were not read.
    stats: Vec<Option<StatColumns>>,
}

impl SegmentZoneMapData {
    pub fn num_rows(&self) -> usize {
        self.paths.len()
    }

    pub fn path(&self, row: usize) -> &str {
        match self.paths.index(row) {
            Some(ScalarRef::String(path)) => path,
            _ => unreachable!("segment zone map path column is a non-null string"),
        }
    }

    pub fn block_count(&self, row: usize) -> u64 {
        match self.block_counts.index(row) {
            Some(ScalarRef::Number(NumberScalar::UInt64(count))) => count,
            _ => unreachable!("segment zone map block_count column is a non-null UInt64"),
        }
    }

    /// Whether the build that wrote the file added this row.
    pub fn is_fresh(&self, row: usize) -> bool {
        match self.fresh.index(row) {
            Some(ScalarRef::Boolean(fresh)) => fresh,
            _ => unreachable!("segment zone map fresh column is a non-null boolean"),
        }
    }

    /// Fill `out` with the known statistics of row `row` among the columns read.
    pub fn fill_stats(&self, row: usize, out: &mut StatisticsOfColumns) {
        out.clear();
        for (column, stats) in self.columns.iter().zip(&self.stats) {
            let Some(stats) = stats else { continue };
            // A NULL null_count marks the statistics as unknown.
            let Some(ScalarRef::Number(NumberScalar::UInt64(nulls))) = stats.null_count.index(row)
            else {
                continue;
            };
            let (min, max) = (stats.min.index(row).unwrap(), stats.max.index(row).unwrap());
            let stats = ColumnStatistics::new(min.to_owned(), max.to_owned(), nulls, 0, None);
            out.insert(column.column_id, stats);
        }
    }
}

fn invalid(path: &str, what: impl Display) -> ErrorCode {
    ErrorCode::StorageOther(format!("invalid segment zone map {path}: {what}"))
}

#[cfg(test)]
mod tests {
    use databend_common_column::types::timestamp_tz;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::i256;
    use opendal::services::Memory;

    use super::*;

    fn schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::String.wrap_nullable()),
            // No min/max statistics for Variant, so it is not covered.
            TableField::new("v", TableDataType::Variant),
        ])
    }

    fn stats(min: Scalar, max: Scalar, null_count: u64) -> ColumnStatistics {
        ColumnStatistics::new(min, max, null_count, 0, None)
    }

    fn int_stats(min: i32, max: i32) -> ColumnStatistics {
        let scalar = |v| Scalar::Number(NumberScalar::Int32(v));
        stats(scalar(min), scalar(max), 0)
    }

    fn string_stats(min: &str, max: &str, null_count: u64) -> ColumnStatistics {
        stats(
            Scalar::String(min.into()),
            Scalar::String(max.into()),
            null_count,
        )
    }

    fn summary(block_count: u64, col_stats: StatisticsOfColumns) -> Statistics {
        Statistics {
            block_count,
            col_stats,
            ..Default::default()
        }
    }

    fn all_column_ids(columns: &[ZoneMapColumn]) -> HashSet<ColumnId> {
        columns.iter().map(|column| column.column_id).collect()
    }

    async fn new_writer(
        op: &Operator,
        name: &str,
        columns: Vec<ZoneMapColumn>,
        ratio: f64,
    ) -> Result<SegmentZoneMapWriter> {
        let location = (name.to_string(), SegmentZoneMapMeta::VERSION);
        SegmentZoneMapWriter::create(op, location, columns, ratio).await
    }

    /// Open and read every covered column of the only row group.
    async fn read_all(op: &Operator, meta: &SegmentZoneMapMeta) -> Result<SegmentZoneMapData> {
        let file = SegmentZoneMapFile::open(op, meta).await?;
        assert_eq!(file.num_row_groups(), 1);
        file.read_row_group(0, &all_column_ids(file.columns()))
            .await
    }

    fn row_of(data: &SegmentZoneMapData, path: &str) -> usize {
        (0..data.num_rows())
            .find(|row| data.path(*row) == path)
            .unwrap()
    }

    fn read_stats(data: &SegmentZoneMapData, path: &str) -> StatisticsOfColumns {
        let mut out = StatisticsOfColumns::new();
        data.fill_stats(row_of(data, path), &mut out);
        out
    }

    /// The zone map keeps min, max and null_count only.
    fn normalize(col_stats: &StatisticsOfColumns) -> StatisticsOfColumns {
        let keep = |s: &ColumnStatistics| stats(s.min.clone(), s.max.clone(), s.null_count);
        col_stats.iter().map(|(id, s)| (*id, keep(s))).collect()
    }

    #[tokio::test]
    async fn test_segment_zone_map_round_trip() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let columns = zone_map_columns(&schema());
        assert_eq!(all_column_ids(&columns), HashSet::from([0, 1]));

        let seg_a =
            StatisticsOfColumns::from([(0, int_stats(1, 10)), (1, string_stats("x", "y", 2))]);
        // All-null column: min/max are NULL but the statistics are known.
        let seg_b = StatisticsOfColumns::from([
            (0, int_stats(20, 30)),
            (1, stats(Scalar::Null, Scalar::Null, 5)),
        ]);
        // Missing statistics and statistics of the wrong type are both unknown.
        let seg_c = StatisticsOfColumns::from([(0, string_stats("1", "2", 0))]);

        let mut writer = new_writer(&op, "zm_1.parquet", columns.clone(), 0.75).await?;
        writer
            .push_summary("seg_a", &summary(1000, seg_a.clone()))
            .await?;
        writer
            .push_summary("seg_b", &summary(7, seg_b.clone()))
            .await?;
        writer.push_summary("seg_c", &summary(1, seg_c)).await?;
        let meta = writer.finish().await?;

        let file = SegmentZoneMapFile::open(&op, &meta).await?;
        assert_eq!(file.columns(), &columns[..]);
        assert_eq!(file.ratio(), 0.75);
        assert_eq!((file.num_rows(), file.num_row_groups()), (3, 1));

        let data = file.read_row_group(0, &all_column_ids(&columns)).await?;
        assert_eq!(data.num_rows(), 3);
        assert_eq!(read_stats(&data, "seg_a"), normalize(&seg_a));
        assert_eq!(read_stats(&data, "seg_b"), normalize(&seg_b));
        assert!(read_stats(&data, "seg_c").is_empty());
        assert_eq!(data.block_count(0), 1000);
        assert_eq!(data.block_count(1), 7);
        assert!((0..3).all(|row| data.is_fresh(row)));

        // Only the requested column is read.
        let data = file.read_row_group(0, &HashSet::from([1])).await?;
        assert_eq!(
            read_stats(&data, "seg_a").into_keys().collect::<Vec<_>>(),
            vec![1]
        );

        // Segment fields alone, as used for the start decision.
        let data = file.read_row_group(0, &HashSet::new()).await?;
        assert!(read_stats(&data, "seg_b").is_empty());
        assert_eq!(data.num_rows(), 3);
        assert_eq!(data.block_count(2), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_segment_zone_map_copy_rows() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let columns = zone_map_columns(&schema());
        let seg_a =
            StatisticsOfColumns::from([(0, int_stats(1, 10)), (1, string_stats("x", "y", 2))]);
        let mut writer = new_writer(&op, "zm_1.parquet", columns.clone(), 1.0).await?;
        writer
            .push_summary("seg_a", &summary(1000, seg_a.clone()))
            .await?;
        let previous = read_all(&op, &writer.finish().await?).await?;

        // Column 1 changed type (string to binary), column 2 is new.
        let mut changed = columns.clone();
        changed[1].data_type = TableDataType::Binary.wrap_nullable();
        changed.push(ZoneMapColumn {
            column_id: 2,
            data_type: TableDataType::Date,
        });
        let seg_b = StatisticsOfColumns::from([(0, int_stats(20, 30))]);
        let mut writer = new_writer(&op, "zm_2.parquet", changed.clone(), 0.5).await?;
        let sources = writer.copy_sources(&previous);
        assert_eq!(sources.0, vec![Some(0), None, None]);
        writer
            .push_summary("seg_b", &summary(3, seg_b.clone()))
            .await?;
        writer
            .push_copy(&previous, row_of(&previous, "seg_a"), &sources)
            .await?;
        let data = read_all(&op, &writer.finish().await?).await?;

        assert_eq!(data.path(0), "seg_b");
        assert!(data.is_fresh(0));
        assert_eq!(data.path(1), "seg_a");
        assert!(!data.is_fresh(1));
        assert_eq!(data.block_count(1), 1000);
        // Only the column that kept its type was copied.
        assert_eq!(
            read_stats(&data, "seg_a"),
            StatisticsOfColumns::from([(0, int_stats(1, 10))])
        );
        assert_eq!(read_stats(&data, "seg_b"), normalize(&seg_b));
        Ok(())
    }

    /// Every type that carries min/max statistics survives the parquet round
    /// trip unchanged: pruning dereferences these scalars by variant.
    #[tokio::test]
    async fn test_segment_zone_map_round_trip_all_stat_types() -> Result<()> {
        use NumberDataType::*;
        use NumberScalar as N;
        use Scalar::Number as Num;
        use TableDataType::Number as NumType;

        let decimal = |precision, scale, value: i128| {
            let size = DecimalSize::new_unchecked(precision, scale);
            let scalar = match DecimalDataType::from(size) {
                DecimalDataType::Decimal64(_) => DecimalScalar::Decimal64(value as i64, size),
                DecimalDataType::Decimal128(_) => DecimalScalar::Decimal128(value, size),
                DecimalDataType::Decimal256(_) => {
                    DecimalScalar::Decimal256(i256::from(value), size)
                }
            };
            Scalar::Decimal(scalar)
        };
        let decimal_type = |precision, scale| {
            TableDataType::Decimal(DecimalSize::new_unchecked(precision, scale).into())
        };
        let ts = |micros, offset| Scalar::TimestampTz(timestamp_tz::new(micros, offset));
        // (type, min, max) per column.
        let cases: Vec<(TableDataType, Scalar, Scalar)> = vec![
            (NumType(Int8), Num(N::Int8(-128)), Num(N::Int8(127))),
            (NumType(UInt8), Num(N::UInt8(0)), Num(N::UInt8(255))),
            (NumType(Int16), Num(N::Int16(-7)), Num(N::Int16(7))),
            (NumType(UInt16), Num(N::UInt16(1)), Num(N::UInt16(65535))),
            (
                NumType(Int32),
                Num(N::Int32(i32::MIN)),
                Num(N::Int32(i32::MAX)),
            ),
            (NumType(UInt32), Num(N::UInt32(0)), Num(N::UInt32(u32::MAX))),
            (
                NumType(Int64),
                Num(N::Int64(i64::MIN)),
                Num(N::Int64(i64::MAX)),
            ),
            (NumType(UInt64), Num(N::UInt64(0)), Num(N::UInt64(u64::MAX))),
            (
                NumType(Float32),
                Num(N::Float32((-1.5f32).into())),
                Num(N::Float32(f32::MAX.into())),
            ),
            (
                NumType(Float64),
                Num(N::Float64((-2.25f64).into())),
                Num(N::Float64(f64::MAX.into())),
            ),
            (
                TableDataType::Date,
                Scalar::Date(-365),
                Scalar::Date(20_000),
            ),
            (
                TableDataType::Timestamp,
                Scalar::Timestamp(-1),
                Scalar::Timestamp(1_700_000_000_000_000),
            ),
            (
                TableDataType::TimestampTz,
                ts(-1, -8 * 3600),
                ts(1_700_000_000_000_000, 8 * 3600),
            ),
            (
                TableDataType::String,
                Scalar::String(String::new()),
                Scalar::String("\u{10FFFF}zzz".into()),
            ),
            (
                decimal_type(10, 2),
                decimal(10, 2, -12345),
                decimal(10, 2, 99999),
            ),
            (
                decimal_type(38, 10),
                decimal(38, 10, i128::MIN / 2),
                decimal(38, 10, i128::MAX / 2),
            ),
            (decimal_type(60, 5), decimal(60, 5, -1), decimal(60, 5, 1)),
        ];

        let mut fields = Vec::new();
        let mut col_stats = StatisticsOfColumns::new();
        for (i, (ty, min, max)) in cases.into_iter().enumerate() {
            fields.push(TableField::new(&format!("c{i}"), ty));
            col_stats.insert(i as ColumnId, stats(min, max, i as u64));
        }
        let columns = zone_map_columns(&TableSchema::new(fields));
        assert_eq!(columns.len(), col_stats.len());

        let op = Operator::new(Memory::default())?.finish();
        let mut writer = new_writer(&op, "zm.parquet", columns.clone(), 1.0).await?;
        writer
            .push_summary("seg", &summary(1, col_stats.clone()))
            .await?;
        let meta = writer.finish().await?;
        let file = SegmentZoneMapFile::open(&op, &meta).await?;
        let data = file.read_row_group(0, &all_column_ids(&columns)).await?;
        assert_eq!(read_stats(&data, "seg"), col_stats);

        // The chunk statistics of a one-row group are that row's statistics,
        // except for the types they do not bound.
        let group_stats = file.row_group_stats(&all_column_ids(&columns));
        let unbounded: Vec<ColumnId> = columns
            .iter()
            .filter(|c| !c.chunk_stats_bound_values())
            .map(|c| c.column_id)
            .collect();
        let bounded_types = |id: &ColumnId| !unbounded.contains(id);
        let mut expected = col_stats.clone();
        expected.retain(|id, _| bounded_types(id));
        assert_eq!(group_stats.len(), 1);
        assert_eq!(group_stats[0], expected);
        let float_or_tz = |ty: &TableDataType| {
            matches!(ty, NumType(Float32 | Float64) | TableDataType::TimestampTz)
        };
        let expected_unbounded: Vec<ColumnId> = columns
            .iter()
            .filter(|c| float_or_tz(&c.data_type))
            .map(|c| c.column_id)
            .collect();
        assert_eq!(unbounded, expected_unbounded);
        Ok(())
    }

    /// Segments written before decimal storage followed the precision keep
    /// 128-bit values for small decimals; the zone map stores them at the width
    /// the column has now.
    #[tokio::test]
    async fn test_segment_zone_map_narrows_wide_decimal_stats() -> Result<()> {
        let size = DecimalSize::new_unchecked(8, 4);
        let schema = TableSchema::new(vec![TableField::new(
            "d",
            TableDataType::Decimal(size.into()),
        )]);
        let wide = |v: i128| Scalar::Decimal(DecimalScalar::Decimal128(v, size));
        let narrow = |v: i64| Scalar::Decimal(DecimalScalar::Decimal64(v, size));
        let col_stats = StatisticsOfColumns::from([(0, stats(wide(-15000), wide(25000), 1))]);

        let op = Operator::new(Memory::default())?.finish();
        let mut writer = new_writer(&op, "zm.parquet", zone_map_columns(&schema), 1.0).await?;
        writer.push_summary("seg", &summary(1, col_stats)).await?;
        let data = read_all(&op, &writer.finish().await?).await?;
        let expected = StatisticsOfColumns::from([(0, stats(narrow(-15000), narrow(25000), 1))]);
        assert_eq!(read_stats(&data, "seg"), expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_segment_zone_map_open_rejects_corrupt_pointer() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let mut writer = new_writer(&op, "zm_1.parquet", zone_map_columns(&schema()), 1.0).await?;
        writer
            .push_summary("seg_a", &summary(1, StatisticsOfColumns::new()))
            .await?;
        let meta = writer.finish().await?;
        let corrupt = |f: fn(&mut SegmentZoneMapMeta)| {
            let mut meta = meta.clone();
            f(&mut meta);
            meta
        };
        for meta in [
            corrupt(|m| m.footer_offset += 1),
            corrupt(|m| m.footer_offset = 0),
            corrupt(|m| m.location.1 += 1),
            corrupt(|m| m.location.0 = "missing.parquet".to_string()),
        ] {
            assert!(
                SegmentZoneMapFile::open(&op, &meta).await.is_err(),
                "{meta:?}"
            );
        }
        Ok(())
    }

    /// Rows beyond [`ROW_GROUP_ROWS`] go to further row groups, each read on
    /// its own; the chunk statistics bound the rows of each group.
    #[tokio::test]
    async fn test_segment_zone_map_row_groups() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let columns = zone_map_columns(&schema());
        let rows = 2 * ROW_GROUP_ROWS + 5;
        let mut writer = new_writer(&op, "zm.parquet", columns.clone(), 1.0).await?;
        for row in 0..rows {
            let group = row / ROW_GROUP_ROWS;
            let value = row as i32;
            let mut col_stats = StatisticsOfColumns::from([(0, int_stats(value, value + 1))]);
            // Group 0: strings known; group 1: one row unknown; group 2: one
            // row entirely NULL, the others known.
            if group != 1 || row != ROW_GROUP_ROWS + 1 {
                col_stats.insert(1, string_stats("k", "k", 0));
            }
            if row == 2 * ROW_GROUP_ROWS + 1 {
                col_stats.insert(1, stats(Scalar::Null, Scalar::Null, 9));
            }
            writer
                .push_summary(&format!("seg_{row}"), &summary(1, col_stats))
                .await?;
        }
        let meta = writer.finish().await?;

        let file = SegmentZoneMapFile::open(&op, &meta).await?;
        assert_eq!((file.num_rows(), file.num_row_groups()), (rows, 3));
        let group_rows = [ROW_GROUP_ROWS, ROW_GROUP_ROWS, 5];
        for (group, expected) in group_rows.into_iter().enumerate() {
            let data = file
                .read_row_group(group, &all_column_ids(&columns))
                .await?;
            assert_eq!(data.num_rows(), expected);
            let first = group * ROW_GROUP_ROWS;
            assert_eq!(data.path(0), format!("seg_{first}"));
            assert_eq!(
                data.path(expected - 1),
                format!("seg_{}", first + expected - 1)
            );
            let first_stats = read_stats(&data, &format!("seg_{first}"));
            assert_eq!(first_stats[&0], int_stats(first as i32, first as i32 + 1));
        }

        let group_stats = file.row_group_stats(&all_column_ids(&columns));
        assert_eq!(group_stats.len(), 3);
        let last = |group: usize| (group_rows[..=group].iter().sum::<usize>() - 1) as i32;
        for (group, stats_of) in group_stats.iter().enumerate() {
            let first = (group * ROW_GROUP_ROWS) as i32;
            assert_eq!(
                stats_of[&0],
                int_stats(first, last(group) + 1),
                "group {group}"
            );
        }
        assert_eq!(group_stats[0][&1], string_stats("k", "k", 0));
        // A row with unknown statistics leaves the column out of its group.
        assert!(!group_stats[1].contains_key(&1));
        // A row that is entirely NULL makes the group nullable.
        assert_eq!(group_stats[2][&1], string_stats("k", "k", 9));
        Ok(())
    }
}
